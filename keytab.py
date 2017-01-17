#!python

from getpass import getuser, getpass

default_keytab = '/home/%s/.kt' % getuser()
default_domain = 'CORP.EPSILON.COM'
ktutil = '/usr/bin/ktutil'

__doc__ = """Keytab file maintenance utility.

Usage:
    keytab.py [-u | --update] <username> [--domain=realm] [--keytab=filename]
                                         [--and-test] [--algorithms=list] [--kvno=entry]
                                         [-d | --debug]
    keytab.py test <username> [--domain=realm] [--keytab=filename]
    keytab.py (-h | --help)

Commands:
    (default)       Creates/overwrites Keytab file
    test            Use generated keytab with kinit to test creating Kerberos ticket.

Arguments:
    <username>      Is your Windows / Active Directory login name
                    (and not UNIX login, in case if it's different from AD login).

Options:
    -h --help            Show this screen
    --update             Overwrites just --kvno keytab entry and leaves other entries the same.
    --domain=realm       Kerberos domain / AD realm [default: %s]
    --keytab=filename    Keytab location [default: %s]
    --and-test           After keytab is created/updated, try to use it by creating a Kerberos ticket
    -d --debug           Print debug information. Note: password is visible in the log output with -d
    --algorithms=list    List of algorithm(s) used for each keytab entry.
                         The list has to be comma-separated [default: rc4-hmac,aes256-cts]
    --kvno=entry         Key entry in keytab, passed as -k kvno argument to
                         ktutil's addent command [default: 1]

Assumptions:
1.    This script expects MIT Kerberos compatible ktutil command
      to be available as %s.
      Script is known not to work with Heimdal Kerberos compatible ktutil.
2.    docopt, pexpect Python modules should be available.

History:
    01/16/2017  rdautkhanov@epsilon.com - 1.0   Initial version
""" % \
          (default_domain, default_keytab, ktutil)


###################################################################################################
###################################################################################################


try:
    import pexpect
    from docopt import docopt
except ImportError:
    exit('Not found doctopt or pexpect Python module. Both are required')
import sys


# Parse command-line arguments
args = docopt(__doc__)

Debug = args['--debug']
keytab = args['--keytab']
principal = args['<username>'] +'@'+ args['--domain']

if Debug: print(args)

def kinit_test ():
    """
    Runs kinit to create a Kerberos ticket using (just) generated keytab file.
    Returns kinit's return code (0 == OK)
    """

    from subprocess import call
    retcode = call(['/usr/bin/kinit', '-kt', keytab, principal], stdout=sys.stdout)
    if retcode == 0:
        print("kinit successfully created Kerberos ticket using this keytab.")
    else:
        print("kinit wasn't able to create Kerberos ticket using this keytab.")
    return retcode

if args['test']:
    sys.exit(kinit_test())


# 0. Start ktutil command as a child process
child = pexpect.spawn(ktutil)
default_prompt = 'ktutil:  '

def wait (prompt=default_prompt):
    ''' Wait for ktutil's prompt
        Returns true if ktutil's cli command  produced output (error message) or unexpected prompt
    '''

    # always wait for default prompt too in case of error, so no timeout exception
    i = child.expect([prompt, default_prompt], timeout=3)

    lines = child.before.strip().split('\n')
    problem = (      len(lines) > 1   # if there is an error message
                or  (i == 1)       # or ktutil gives default prompt when another prompt expected
              )
    if problem:
        print('ktutil error: ' + lines[1])
    return problem

# wait for ktutil to show its first prompt
wait()
if Debug:
    child.logfile = sys.stdout
    print('Spawned ktutil successfully.')

# 1. if it's an update, then read in keytab first
wkt_action = 'save'
if args['--update']:
    wkt_action = 'update'
    child.sendline('read_kt ' + keytab)
    if wait():
        print("Couldn't read keytab file %s\nNew file will be created instead" % keytab)
    # TODO: if KVNO already exists, ktutil may duplicate records in that entry
else:
    # else - try removing existing keytab
    from os import remove
    try:
        remove(keytab)
        if Debug:
            print('Existing keytab %s removed.' % keytab)
    except OSError:
        pass        # assuming e.errno==ENOENT  - file doesn't exist

# 2. Prompt user for Principal's password
password = getpass('Active Directory user %s password: ' % principal)

# 3. For each algorithm, call ktutil's addent command
for algorithm in args['--algorithms'].split(','):

    child.sendline('addent -password -p %s -k %s -e %s'
                        % (principal, args['--kvno'], algorithm)
                  )
    if wait('Password for ' + principal):
        exit('Unexpected ktutil error while waiting for password prompt')

    child.sendline(password)
    if wait():
        exit('Unexpected ktutil error after addent command')

# 4. Now we can save keytab file
child.sendline('write_kt ' + keytab)
if wait():
    exit("Couldn't write keytab file " + keytab)
print("Keytab file %s %sd." % (keytab, wkt_action))

# 5. exit from ktutil
child.sendline('quit')
child.close()           # termintate ktutil (if it's not closed already)

# 6. Optionally test newly created/update keytab
if args['--and-test']:
    kinit_test()



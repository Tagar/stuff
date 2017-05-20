#!python

from getpass import getuser

default_notebooks_dir = '/home/%s/zeppelin/notebooks' % getuser()

__doc__ = """ Zeppelin notebooks helper tool.

Usage:
    znote.py extract [--fetch=interpreter] [--notebooks-dir=directory]
                [--skip-select] [--skip-noop] [--no-comments] [--get-disabled]
                [--default-interpreter=interpreter] [--sqlc-var=sqlc] [--imports] <noteid>
    znote.py clean-output [--notebooks-dir=directory] <noteid>
    znote.py (-h | --help)

Commands:
    extract             Extract code of type --type (defaults to Python)
    clean-output        Removes output from all paragraphs
                        (good to do before source rep commits and code reviews)

Examples:
    python ./znote.py extract 2CAWV29G2
    python ./znote.py clean-output 2C9CTUGT3

Arguments:
    <noteid>            Note ID - see directories under %s
                        (also visible in URI when that note is open in Zeppelin)

Options:
    --fetch=interpreter    Defines which language/interpreter to fetch. [default: pyspark]
    --notebooks-dir=dir Directory that stores all Zeppelin notes.
                                    [default: %s]
    --no-comments       Don't add comments formed from paragraph titles
                        and markdown paragraphs
    --get-disabled      Produce code even for disabled paragraphs
    --skip-selects      Adds heuristics that detect spark-sql paragraphs that
                        just selects data (and not for example alters tables), and skips those
    --skip-noop         Adds heuristics that detect no-op paragraphs (like to display data
                        or schema) and skip those paragraphs
    --default-interpreter=interpreter
                        Sets default interpreter type as configured in Zeppelin.
                        Because it's possible to not set paragraph type each time for default
                        interpreter type, this script has to know what's default [default: pyspark]
    --sqlc-var=sqlc     SQL Context variable [default: sqlc]
    --imports           Add a header to embed necessary Spark import
    -h --help           Show this screen

Know bugs / todo-s:
    1. This script should work with --fetch other than pyspark, but it was tested only with pyspark.
       Some options like --skip-noop should be tweaked for other languages other than pyspark
    2. Script will be fed to stdout - make an optional parameter to write/append to a file.

History:
    03/07/2017  rdautkhanov@epsilon.com - 1.0   Initial version
""" % \
            (default_notebooks_dir, default_notebooks_dir)

###################################################################################################
###################################################################################################


import json
import re
from os.path import isdir, isfile

try:
    from docopt import docopt       # docopt isn't part of Anaconda - so checking just this pkg
except ImportError:
    exit('doctopt Python package not found.')


# Parse command-line arguments using above pattern/description block
args = docopt(__doc__)
## print(args)

noteid = args['<noteid>']

notebooks_dir = args['--notebooks-dir']
note_dir = notebooks_dir + '/' + noteid
json_name = 'note.json'
json_file = note_dir + '/' + json_name

if not isdir(notebooks_dir):    exit("Directory specified in --notebooks-dir doesn't exist (%s)" % notebooks_dir)
if not isdir(note_dir):         exit("Note directory doesn't exist (%s). Check if NoteID is correct" % note_dir)
if not isfile(json_file):       exit("%s file doesn't exist where expected: %s" % (json_name, json_file))

with open(json_file) as data_file:
    data = json.load(data_file)

assert data['id'] == noteid, "Unexpected note id in " + json_file

if args['extract']:

    fetch_intp = args['--fetch']
    default_intp = args['--default-interpreter']
    sqlc = args['--sqlc-var']
    comments = not args['--no-comments']

    if args['--imports']:
            from textwrap import dedent
            print(dedent('''
                    from pyspark.sql import HiveContext
                    from pyspark import SparkConf, SparkContext

                    # start spark application context with application name = note title
                    conf = SparkConf().setAppName('%s')
                    sc = SparkContext(conf=conf)

                    # after Spark context started, reduce logging level to ERROR:
                    log4j = sc._jvm.org.apache.log4j
                    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

                    ## sqlc = HiveContext(sc)           # this should be part of the notebook

            ''' %  data['name']))

    print "## Fetching %s code from Zeppelin note '%s', id %s" % (fetch_intp, data['name'], noteid)

    for p in data['paragraphs']:

        print

        if comments:
            print '## Paragraph %s:' % p['id']

        # Check if paragraph is disabled
        enabled = p['config'].get('enabled', True)      # by default assume enabled
        if not enabled:
            if args['--get-disabled']:
                if comments: print "## paragraph '%s' is disabled but will run because of --get-disabled" % p['id']
            else:
                if comments: print "## paragraph '%s' is disabled and will be skipped" % p['id']
                continue

        # print title
        title = p.get('title', '')
        if title and comments:
            print "## **** %s ****" % title

        text = p.get('text', '')                        # empty text '' is the default

        # Zeppelin thing - 'scala' is default irrespective of default interpreter setting
        # para_type = p['config'].get('editorMode', 'ace/mode/scala').split('/')[2]       # takes last word
        # So editorMode is misleading and below code will infer type from the text instead..

        # Detect paragraph type
        para_type_re = '^\s*%(\w+)\s+'
        m = re.match(para_type_re, text, flags=re.MULTILINE)
        if m:
            para_type = m.group(1)
            text = re.sub(para_type_re, '', text, count=1, flags=re.MULTILINE)      # remove para type from the code too
        else:
            para_type = default_intp

        # if comments: print "## (paragraph type is %s)" % para_type

        if re.match('^\s*$', text):
            if comments: print "## (no code)"
            continue

        # Now we can append code of the current paragraph
        if para_type == fetch_intp:

            # first, check if it's a no-op command that we should skip
            if args['--skip-noop'] and fetch_intp=='pyspark' and re.match(
                        r''' \A \s *                                 # any whitespace before code
                                (  \w + \. (show|printSchema) \( \)           # SKIP dataframe.show() or printSchema
                                |  z \. show \( . + \)                        # OR z.show( dataframe )
                                |  print \s * \(? .+ \)?                      # or a print statement
                                )
                              \s * \Z                                # any whitespace before end of the code
                         '''
                        , text
                        , flags=re.VERBOSE
                        ):
                    if comments: print("## skipping no-op code because of --skip-noop")
                    continue  # skip to next paragraph

            print text

        elif para_type == 'sql':

            # first, check if it's a pure select statement that we should skip
            if args['--skip-select'] and re.match(
                        r'''^( \s*        # optional comment block:
                              --            # sql syntax comment - double hyphen
                                .*  \n      # commented line up to newline is ignored
                             | \s*  \n      # OR empty line - since commented and empty lines can interleave
                             )*           # 0 or more commented or empty lines
                               \s*        # ignore spaces before `select` keyword
                           (SELECT|explain) \s+     # `select` or `explain` keyword followed by whitespace
                         '''
                        , text
                        , flags=re.IGNORECASE|re.VERBOSE
                        ):
                if comments: print("## skipping SELECT/explain statement because of --skip-select")
                continue  # skip to next paragraph

            print "%s.sql(''' %s ''')" % (sqlc, text)

        elif para_type == 'md':

            text = re.sub('^', '# ', text, flags=re.MULTILINE)      # show MarkDown as-is, just commented
            print text

        elif para_type == 'sh':

            # TODO: 

            continue

        else:
            if comments: print("## skipping non-target language '%s'" % para_type)

exit()

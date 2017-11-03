
# This function implements copyMerge from Hadoop API
# copyMerge will be deprecated in Hadoop 3.0
# This can be used in a pySpark application (assumes `sc` variable exists)

def copyMerge (src_dir, dst_file, overwrite=False, deleteSource=False, debug=False):
    
    # this function has been migrated to https://github.com/Tagar/abalon Python package
    
    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)

    # check files that will be merged
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))
    files.sort()

    # dst_permission = hadoop.fs.permission.FsPermission.valueOf(permission)      # , permission='-rw-r-----'
    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for file in files:
            if debug: 
                print("Appending file {} into {}".format(file, dst_file))

            in_stream = fs.open(file)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if deleteSource:
        fs.delete(hadoop.fs.Path(src_dir), True)    # True=recursive
        if debug:
            print("Source directory {} removed.".format(src_dir))

copyMerge('/user/rdautkha/testdir', '/user/rdautkha/test_merge.txt', debug=True, overwrite=True, deleteSource=True)


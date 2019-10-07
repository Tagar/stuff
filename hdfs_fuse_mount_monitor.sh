#!/usr/bin/bash

FUSE_MOUNT="/hdfs_mount"

access_fuse() {
        ls $FUSE_MOUNT >/dev/null 2>&1
}

fix_fuse() {

        {
                echo "Bouncing hdfs fuse mount $FUSE_MOUNT"

                sudo fusermount -uz $FUSE_MOUNT
                sleep 1
                sudo mount $FUSE_MOUNT

        } | mailx -s "HDFS fuse mount bounced on `hostname -s`" rdautkhanov@epsilon.com

}

access_fuse &
access_fuse_pid=$!

waited_seconds=0
max_wait_seconds=20

while kill -0 "$access_fuse_pid" >/dev/null 2>&1; do
        ## echo "PROCESS pid=$access_fuse_pid IS RUNNING"

        if [ $waited_seconds -gt $max_wait_seconds ]
        then
                fix_fuse

                kill $access_fuse_pid
                sleep 1
                kill -9 $access_fuse_pid

                exit 1
        fi

        sleep 1
        ((waited_seconds++))
done

## echo "PROCESS $access_fuse_pid TERMINATED"
exit 0

## TODO: 
## 1. the script assumes HDFS fuse mount is mounted, but could be hanging;
##    improve by checking if the mount isn't mounted at all.


#!/bin/sh

if [ ! -d $TEMP_DIR ]; then
   echo "lock.sh: TEMP_DIR ($TEMP_DIR) is not a directory"
   exit 1
fi

# define a locking function
Lock() { 
   if [ -n "$LOCK_NAME" ]; then
      ME="`basename $LOCK_NAME`"
   else
      ME="`basename $1`"
   fi
   LOCK_TMP="$TEMP_DIR/lock.sh$$.$ME"
   LOCK_FILE="$TEMP_DIR/lock.sh.$ME"
 
   trap "" 1 2 3 9 15 
   if echo $$ > $LOCK_TMP; then :; else exit 1; fi       # create temporary file
   ln -n $LOCK_TMP $LOCK_FILE > /dev/null 2>&1           # link lock file to temp file
   if [ "`ls -l $LOCK_TMP | awk '{print $2}'`" = "2" ]; then  # check link count on temp file
      rm -f $LOCK_TMP                                    # link count is two, we got a lock
      trap "OnExit" 0 1 2 3 9 15 
   else 
      rm -f $LOCK_TMP                                    # someone else got the lock
      # check for stale lock file
      pid="`head -1 $LOCK_FILE`" 2>/dev/null
      if kill -0 $pid > /dev/null 2>&1; then
         echo "lock.sh: another instance of $ME is already running."
      else
         echo "lock.sh: removing stale lock file for $ME."
         rm -f $LOCK_FILE;
      fi
      exit 0 
   fi 
} 
 
# define an exit function (cleans up lock file on a normal death)
OnExit() { 
   trap 0 1 2 3 9 15 
   rm -f $LOCK_TMP $LOCK_FILE 
   exit 0 
} 

Lock $1                       # lock based on arg 1
PATH="$SAVEPATH"; export PATH # restore PATH
"$@"                          # run program atomically


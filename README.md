# yarn-memory-tracker
This is a shell script to analyze memory usage of a Yarn application.

Prerequisites:
- Proper setup of env variable $HADOOP_HOME and $HADOOP_YARN_HOME
- A shared log directory at $HADOOP_YARN_HOME/logs to which all Yarn machines writes log to.
- App specific logs under $HADOOP_YARN_HOME/logs/userlogs

If the log directories are not setup as above, the script will have to be modified to accomodate non-standard directory or distributed log files.

To use the script, find the appID of the app and run

yarn-memory-tracker.sh appID

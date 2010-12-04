#!/bin/sh

display_help() {
  echo "usage: $0 [-h] [-r <m2 dir>]"
  exit
}

error() {
  msg="$1"
  shift

  [ -z "${msg}" ] && msg="(No message provided)"

  echo "Error: $msg"
  exit 1
}

while [ -n "$*" ] ; do
  arg="$1"
  shift

  case "$arg" in
    -h)
      display_help
      ;;
    -r)
      REPO_PATH="$1"
      shift
      ;;
    *)
      error "Unknown argument $arg"
      ;;
  esac
done

if [ -z "${REPO_PATH}" ] ; then
  REPO_PATH="${HOME}/.m2/repository"
fi

echo "Attempting to setup thrift maven plugin"

if [ ! -d "${REPO_PATH}/org/apache/thrift/tools/maven-thrift-plugin/0.1.9-SNAPSHOT" ] ; then
  echo "Installing thrift maven plugin"
  cd /tmp || error $!
  git clone git://github.com/kimballa/maven-thrift-plugin.git || error $!
  cd maven-thrift-plugin || error $!
  mvn install || error $!
fi

if [ ! -d "${REPO_PATH}/org/apache/thrift/tools/maven-thrift-plugin/0.1.9-SNAPSHOT" ] ; then
  echo "Installing thrift maven plugin"
  cd /tmp || error $!
  git clone git://github.com/kimballa/maven-thrift-plugin.git || error $!
  cd maven-thrift-plugin || error $!
  mvn install || error $!
fi

echo "Injecting libs into your maven repo (${REPO_PATH}) that aren't available publicly."

if [ ! -d "${REPO_PATH}/com/cloudera/org.apache.thrift/0.4.0" ] ; then
  echo "inject: com.cloudear:org.apache.thrift:0.4.0"
  mvn install:install-file -DgroupId=com.cloudera -DartifactId=org.apache.thrift \
    -Dversion=0.4.0 -Dpackaging=jar -DgeneratePom -Dfile=lib/libthrift-0.4.0.jar
fi

if [ ! -d "${REPO_PATH}/com/cloudera/org.schwering.irc/1.0.0" ] ; then
  echo "inject: com.cloudear:org.schwering.irc:1.0.0"
  mvn install:install-file -DgroupId=com.cloudera -DartifactId=org.schwering.irc \
    -Dversion=1.0.0 -Dpackaging=jar -DgeneratePom -Dfile=lib/irclib.jar
fi

echo "Build environment should be sane. Try 'mvn package'."


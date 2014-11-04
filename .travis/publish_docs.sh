#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

  echo -e "Publishing scaladoc...\n"

  mvn scala:doc

  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/hammerlab/guacamole gh-pages > /dev/null

  cd gh-pages
  git rm -rf ./docs
  mv ../target/site/scaladocs/ ./docs
  git add -f .
  git commit -m "Lastest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
  git push -fq origin gh-pages > /dev/null

  echo -e "Published scaladoc to gh-pages.\n"
  
fi

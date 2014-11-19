#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

  echo -e "Generating scaladoc and coverage report.\n"

  mvn scala:doc
  mvn scoverage:report

  echo -e "Publishing scaladoc and coverage report.\n"

  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/hammerlab/guacamole gh-pages > /dev/null

  cd gh-pages
  git rm -rf ./docs
  git rm -rf ./scoverage-report
  mv ../target/site/scaladocs ./docs
  mv ../target/scoverage-report ./scoverage-report
  git add -f .
  git commit -m "Lastest doc and coverage report on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
  git push -fq origin gh-pages > /dev/null

  echo -e "Published scaladoc to gh-pages.\n"
  
fi

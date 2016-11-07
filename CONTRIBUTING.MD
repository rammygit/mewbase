# Guide to contributing

Thank you for considering contributing to mewbase!

We value contributions, no matter how small, from anyone.

Please read this if you intend to contribute to the project.

## What to contribute?

* Have a look at the GitHub issues - is there something that catches your eye?
* Ask on the [google group](https://groups.google.com/forum/#!forum/mewbase) for something to do - one of the permanent team should be able to point you 
in the direction of an appropriate task
* If you haven't contributed to the project before, best to start with something small

## General principles

* All contributions should be submitted via pull requests
* The master branch should be stable at all times (all green tests) - do not work on the master branch directly
* The exception to the above is for very small or cosmetic changes of < 10 lines or so can be applied directly on master

## Before committing

* Please sign the [Contributor License Agreement](CLA.txt) - for now this can be done electronically by sending an email to the project
lead. We will automate this going ahead

## Making your changes

* Fork the repository on GitHub
* Create a new branch for your changes
* Make your changes
* Please make sure the code conforms to the code style guidelines
* Make sure you have good test coverage for any new features or fixes
* Any new interfaces should have javadoc
* Make sure the test suite passes after your changes
* Commit your changes into that branch
* Use descriptive and meaningful commit messages
* If you have a lot of commits squash them into a single commit
* Push your changes to your branch in your forked repository
* Submit a pull request via the normal GitHub UI.

## Review process

* Contributions must be reviewed by at least one other developer. Other developers are welcome to make comments on the PR too.
The review should be carried out by placing comments on the PR in GitHub, not by sending private emails or other means.
 This provides visibility and provides a public record.
* Review comments may well require further changes to the code before the PR is accepted. This is *completely normal*,
so please don't be offended by it. Do not expect PRs to be accepted without further changes.
* Make those changes and tell the reviewer by adding a comment on the PR that you have made them and are ready for the
changes to be reviewed (e.g. @joebloggs hey I made those changes we discussed, can you take a look again?).
Do not expect the reviewer to know when you have made any further changes and are ready to have them reviewed without
prompting them first.
* Repeat the above to and fro until there is concensus the work is ready to be merged into master. Once that point is
reached, it can be merged.
* Concensus is always preferable, but if concensus on an approach cannot be reached, the Project Lead should decide on
which approach to take.
 
## After submitting

* Do not use your branch for any other development, otherwise further changes that you make will be visible in the PR.



# cscTimingTool
CSC Timing table generator from EMTF DQM

This is a simplified version of the CSC timing website that only generates a timing table both as a printout in your terminal as well as a csv file to upload to another tool such as google sheets.

# Instructions

You should run this in lxplus.

1. The first requirement is to get a CERN API for accessing Run Registry: https://github.com/CMSTrackerDPG/cernrequests

Follow the instructions under `For CERN APIs using the ""new"" SSO` and modify the `.env` file with your credentials.

2. Your lxplus should have your **unencrypted** `usercert.pem` and `userkey.pem` GRID certificates in your `~/private` or `~/.globus/` area. For alternate uses and more explanations on this see https://github.com/CMSTrackerDPG/cernrequests

3. When these are ready, the rest can be done as follows:

```
pip install -r requirements.txt
python3 cscTiming.py --runs <run range> --minLS <value> --runClass <run class in runregistry>
```

There is a `--help` option with more information on these parameters. The content is below:
```
optional arguments:
  -h, --help           show this help message and exit
  --runs RUNS          list of run numbers separated by comma or range separated by colon
  --minLS MINLS        minimum LS duration of the runs
  --runClass RUNCLASS  Run class in runregistry. Could be Collisions, CollisionsHI, Cosmics, or Commissioning
```

Example command:
```
python3 cscTiming.py --runs 387833:387860 --minLS 100 --runClass CollisionsHI
```

Please contact Efe if you have any further questions.

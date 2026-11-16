# BKK Delay Radar

## District reliability index and live traffic (optional extras)

    bash deploy.sh districts                     # stop -> district table + district map, from OpenStreetMap
    TOMTOM_KEY="..." bash deploy.sh tomtom       # live road traffic switch on the delay map

The district index scores each of Budapest's 23 districts from 1 (worst) to 10 (best) on five measures
(on time 30 %, typical delay 20 %, extra time to plan 20 %, bunching 15 %, delay added per km 15 %),
scaled between the worst and the best district (MinMax) and weighted. The weights are a choice and are
shown on the page next to the measures.

TomTom traffic is only displayed: the viewer's browser loads the tiles straight from TomTom, and nothing
from TomTom is collected or stored, because TomTom's traffic terms do not allow storing traffic data.
The key in tomtom.json is readable by anyone who opens the page: use a free key without billing details.

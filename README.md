# kachok
A simple json-nd data pumper to elasticsearch; no backpressure support.
Use it when logstash fails you or when you need a solid base to start from.


```
            _                             _
           | |                           | |
         =H| |========mn=======nm========| |H=
           |_|        ( \     / )        |_|
                       \ )(")( /
                       ( /\_/\ )
                        \     /
                         )=O=(
                        /  _  \
                       /__/ \__\
                       | |   | |
                       |_|   |_|
                       (_)   (_)
======================================================David Riley
```



```
py -3 kachok.py pumpJSONND --help
WARNING: ujson not found, install it for better performance
INFO: Showing help with the command 'kachok.py pumpJSONND -- --help'.

NAME
    kachok.py pumpJSONND

SYNOPSIS
    kachok.py pumpJSONND SELF INDEX <flags> [LOGFILES]...

POSITIONAL ARGUMENTS
    SELF
    INDEX
    LOGFILES

FLAGS
    --batchsize=BATCHSIZE (required)
    --doctype=DOCTYPE (required)
    --errordir=ERRORDIR (required)

NOTES
    You can also use flags syntax for POSITIONAL ARGUMENTS
    
  ``

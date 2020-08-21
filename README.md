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
NAME
    kachok.py pumpJSONND - Pump new-line delimited JSON documents to elasticsearch

SYNOPSIS
    kachok.py pumpJSONND SELF INDEX <flags> [LOGFILES]...

DESCRIPTION
    Pump new-line delimited JSON documents to elasticsearch

POSITIONAL ARGUMENTS
    SELF
    INDEX
        elasticsearch index
    LOGFILES
        list of files to import

FLAGS
    --batchsize=BATCHSIZE
        batch size per request
    --doctype=DOCTYPE
        document type
    --errordir=ERRORDIR
        directory where to output errors (if unspecified, outputs to same directory as file)
    --progress=PROGRESS
        display progress bar
  ```

# Examples

## Make Index 
```./kachok.py --endpoint=http://localhost:9200 makeIndex myindexname```

## Upload data 
```./kachok.py --endpoint=http://localhost:9200 pumpJSONND myindexname /path/to/files/*.json```

## Authenticate using HTTP basic auth 
```./kachok.py --username true --password groot --endpoint=http://localhost:9200 pumpJSONND myindexname /path/to/files/*.json```

You can use glob syntax (e.g '**' for recursive)

# optional_flattening_of_json
  Flattening of the json to push the data in InflluxDB and have a option to skip flattening if it is not required.

# prerequisite to run the code.
  Install the [Influxdb](https://docs.influxdata.com/influxdb/v1.7/introduction/installation/) using the link and start
  the server as mentioned in the article.

# Steps to run the go code.

1. Install the go binary.

2. Include the following lines in the $HOME/.profile file

   ```
     export GOPATH=$HOME/go
     export PATH=/usr/local/go/bin:$GOPATH/bin:$GOPATH/bin/golint
   ```

3. Install the influxdb go client library using the following commands

   ```
     export INFLUXDB_GO_PATH=${GOPATH}/src/github.com/influxdata/influxdb
     mkdir -p ${INFLUXDB_GO_PATH} && \
     git clone https://github.com/influxdata/influxdb ${INFLUXDB_GO_PATH} && \
     cd ${INFLUXDB_GO_PATH} && \
     git checkout -b v1.6.0 tags/v1.6.0
   ```

4. Use the following command to run the go example,

   ```
     go run influx_flat_json.go
   ```

# Steps to run the python code.

1. Install the influxdb python library using the following command.

   ```
     pip3 install influxdb
   ```

2. To run the python example,

   ```
     python3 influx_flat_json.py
   ```

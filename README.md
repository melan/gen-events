# Generate Events to test streams processing

This package implements 5 basic scenarios:

1. Heartbeat messages
2. Noisy Errors
3. Temperature readings
4. Temperature readings with broken sensors
5. Random changes.

## How to run

`./gen-events --help` to get a help message

This example will run all of the use cases with 10 orgs per use case, it'll cleanup all resources at the end, 
will output results into files in the current directory. Interval between events is 30 seconds:
 
```bash
    ./gen-events  --interval 30 \
        --case-id heartbeat_message \
        --case-id structured_error_message \
        --case-id temperature_reading \
        --case-id broken_temperature_reading \
        --case-id data_change \
        --output file \
        --orgs-count 10 --cleanup
```

If you don't want to build and run this tool locally you can use a docker container `melan/gen-events`. The last example
but with the docker container will look like this:

```bash
docker run -p 8080:8080 -v `pwd`:/output melan/gen-events \
        --interval 30 \
        --case-id heartbeat_message \
        --case-id structured_error_message \
        --case-id temperature_reading \
        --case-id broken_temperature_reading \
        --case-id data_change \
        --output file \
        --output-path /output \
        --orgs-count 10 \
        --cleanup
```

When the tool runs with `--orgs-count 1` - it's possible to define size of the org using `--org-size <org size>` parameter.
If there are more the `--orgs-count` parameter has value more than 1 - sizes of the Orgs will be selected proportionally.

When the application is running it exposes Prometheus metrics endpoint on port `8080` under path `/metrics`. 
The port can be changed using `--listen-address` parameter

To run the tool with output to AWS Kinesis please call it with `--output kinesis`, make sure to provide all AWS_* 
environment variables to give it access to a user with full access to the AWS Kinesis.   
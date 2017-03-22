def aml_cli_get_sample_request():
    sampleDF = spark.read.schema(inputSchema).json('PLACEHOLDER')
    sampleRequest = sampleDF.first()
    ret_val = ''
    for column in sampleDF.columns:
        ret_val = ret_val + str(sampleRequest[column]) + ','

    return '[[{}]]'.format(ret_val.strip(','))

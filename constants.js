/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

/*
 * May 2015
 *
 * Derivative created by HP, to leverage and extend the function framework to provide automatic loading from S3, via
 * Lambda, to the HP Vertica Analytic Database platform. This derivative work remains governed by the Amazon
 * Software License, and is subject to all terms and restrictions noted in ASL.
 *
 */


batchId = 'batchId';
currentBatch = 'currentBatch';
s3prefix = 's3Prefix';
lastUpdate = 'lastUpdate';
complete = 'complete';
locked = 'locked';
open = 'open';
error = 'error';
entries = 'entries';
status = 'status';
configTable = 'LambdaVerticaBatchLoadConfig';
batchTable = 'LambdaVerticaBatches';
batchStatusGSI = 'LambdaVerticaBatchStatus';
filesTable = 'LambdaVerticaProcessedFiles';
conditionCheckFailed = 'ConditionalCheckFailedException';
provisionedThroughputExceeded = 'ProvisionedThroughputExceededException';

/* defaults for setup.js */
REQD_BLANK = 'Reqd.';
OPTIONAL_BLANK = 'Optional';

dfltRegion = 'us-east-1';
dfltS3Prefix = REQD_BLANK ;
dfltS3MountDir = '/mnt/s3/';
dfltFilenameFilter = '.*\\.csv';
dfltClusterEndpoint = REQD_BLANK ;
dfltClusterPort = '5433';
dfltUserName = REQD_BLANK ;
dfltUserPwd = REQD_BLANK ;
dfltTable = REQD_BLANK ;
dfltCopyOptions = OPTIONAL_BLANK ;
dfltPreLoadStatement = OPTIONAL_BLANK ;
dfltPostLoadStatement = OPTIONAL_BLANK ;
dfltBatchSize = '1';
dfltBatchTimeoutSecs = '30';
dfltFailureTopic = OPTIONAL_BLANK ;
dfltSuccessTopic = OPTIONAL_BLANK ;



INVALID_ARG = -1;
ERROR = -1;
OK = 0;

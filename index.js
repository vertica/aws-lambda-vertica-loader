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


var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
	region = "us-east-1";
	console.log("AWS Lambda Vertica Database Loader using default region " + region);
}

var aws = require('aws-sdk');
aws.config.update({
	region : region
});
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : region
});
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : region
});
var sns = new aws.SNS({
	apiVersion : '2010-03-31',
	region : region
});
require('./constants');
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');
var vertica = require('vertica');
var upgrade = require('./upgrades');

// main function for AWS Lambda
exports.handler =
		function(event, context) {
			/** runtime functions * */

			/*
			 * Function which performs all version upgrades over time - must be able
			 * to do a forward migration from any version to 'current' at all times!
			 */
			exports.upgradeConfig = function(s3Info, currentConfig, callback) {
				// v 1.x to 2.x upgrade for multi-cluster loaders
				if (currentConfig.version !== pjson.version) {
					upgrade.upgradeAll(dynamoDB, s3Info, currentConfig, callback);
				} else {
					// no upgrade needed
					callback(null, s3Info, currentConfig);
				}
			};

			/* callback run when we find a configuration for load in Dynamo DB */
			exports.foundConfig =
					function(s3Info, err, data) {
						if (err) {
							console.log(err);
							var msg = 'Error getting Vertica Configuration for ' + s3Info.prefix + ' from Dynamo DB ';
							console.log(msg);
							context.done(error, msg);
						}

						if (!data || !data.Item) {
							// finish with no exception - where this file sits
							// in the S3
							// structure is not configured for loads
							console.log("No Configuration Found for " + s3Info.prefix);

							context.done(null, null);
						} else {
							console.log("Found Vertica Load Configuration for " + s3Info.prefix);

							var config = data.Item;
							var thisBatchId = config.currentBatch.S;

							// run all configuration upgrades required
							exports.upgradeConfig(s3Info, config, function(err, s3Info, config) {
								if (err) {
									console.log(err);
									context.done(error, err);
								} else {
									if (config.filenameFilterRegex) {
										if (s3Info.key.match(config.filenameFilterRegex.S)) {
											exports.checkFileProcessed(config, thisBatchId, s3Info);
										} else {
											console.log('Object ' + s3Info.key + ' excluded by filename filter \''
													+ config.filenameFilterRegex.S + '\'');

											// scan the current batch to decide
											// if it needs to
											// be
											// flushed due to batch timeout
											exports.processPendingBatch(config, thisBatchId, s3Info);
										}
									} else {
										// no filter, so we'll load the data
										exports.checkFileProcessed(config, thisBatchId, s3Info);
									}
								}
							});
						}
					};

			/*
			 * function to add a file to the pending batch set and then call the
			 * success callback
			 */
			exports.checkFileProcessed = function(config, thisBatchId, s3Info) {
				var itemEntry = s3Info.bucket + '/' + s3Info.key;

				// perform the idempotency check for the file
				var fileEntry = {
					Item : {
						loadFile : {
							S : itemEntry
						}
					},
					Expected : {
						loadFile : {
							Exists : false
						}
					},
					TableName : filesTable
				};

				// add the file to the processed list
				dynamoDB.putItem(fileEntry, function(err, data) {
					if (err) {
						// the conditional check failed so the file has already
						// been
						// processed
						console.log("File " + itemEntry + " Already Processed");
						context.done(null, null);
					} else {
						if (!data) {
							var msg = "Idempotency Check on " + fileEntry + " failed";
							console.log(msg);
							exports.failBatch(msg, config, thisBatchId, s3Info, undefined);
						} else {
							// add was OK - proceed with adding the entry to the
							// pending batch
							exports.addFileToPendingBatch(config, thisBatchId, s3Info, itemEntry);
						}
					}
				});
			};

			/**
			 * Function run to add a file to the existing open batch. This will
			 * repeatedly try to write and if unsuccessful it will requery the batch
			 * ID on the configuration
			 */
			exports.addFileToPendingBatch =
					function(config, thisBatchId, s3Info, itemEntry) {
						console.log("Adding Pending Batch Entry for " + itemEntry);

						var proceed = false;
						var asyncError = undefined;
						var addFileRetryLimit = 100;
						var tryNumber = 0;

						async
								.whilst(
										function() {
											// return OK if the proceed flag has
											// been set, or if we've hit the
											// retry count
											return !proceed && tryNumber < addFileRetryLimit;
										},
										function(callback) {
											tryNumber++;

											// build the reference to the
											// pending batch, with an
											// atomic add of the current file
											var item = {
												Key : {
													batchId : {
														S : thisBatchId
													},
													s3Prefix : {
														S : s3Info.prefix
													}
												},
												TableName : batchTable,
												UpdateExpression : "add entries :entry set #stat = :open, lastUpdate = :updateTime",
												ExpressionAttributeNames : {
													"#stat" : 'status'
												},
												ExpressionAttributeValues : {
													":entry" : {
														SS : [ itemEntry ]
													},
													":updateTime" : {
														N : '' + common.now(),
													},
													":open" : {
														S : open
													}
												},
												/*
												 * current batch can't be locked
												 */
												ConditionExpression : "#stat = :open or attribute_not_exists(#stat)"
											};

											// add the file to the pending batch
											dynamoDB.updateItem(item, function(err, data) {
												if (err) {
													if (err.code === conditionCheckFailed) {
														/*
														 * the batch I have a reference to was locked so
														 * reload the current batch ID from the config
														 */
														var configReloadRequest = {
															Key : {
																s3Prefix : {
																	S : s3Info.prefix
																}
															},
															TableName : configTable,
															ConsistentRead : true
														};
														dynamoDB.getItem(configReloadRequest, function(err, data) {
															if (err) {
																console.log(err);
																callback(err);
															} else {
																/*
																 * reset the batch ID to the current marked
																 * batch
																 */
																thisBatchId = data.Item.currentBatch.S;

																/*
																 * we've not set proceed to true, so async will
																 * retry
																 */
																console.log("Reload of Configuration Complete after attempting Locked Batch Write");

																/*
																 * we can call into the callback immediately, as
																 * we probably just missed the pending batch
																 * processor's rotate of the configuration batch
																 * ID
																 */
																callback();
															}
														});
													} else {
														asyncError = err;
														proceed = true;
														callback();
													}
												} else {
													/*
													 * no error - the file was added to the batch, so mark
													 * the operation as OK so async will not retry
													 */
													proceed = true;
													callback();
												}
											});
										},
										function(err) {
											if (err) {
												// throw presented errors
												console.log(err);
												context.done(error, err);
											} else {
												if (asyncError) {
													/*
													 * throw errors which were encountered during the
													 * async calls
													 */
													console.log(asyncError);
													context.done(error, asyncError);
												} else {
													if (!proceed) {
														/*
														 * process what happened if the iterative request to
														 * write to the open pending batch timed out
														 * 
														 * TODO Can we force a rotation of the current batch
														 * at this point?
														 */
														var e =
																"Unable to write "
																		+ itemEntry
																		+ " in "
																		+ addFileRetryLimit
																		+ " attempts. Failing further processing to Batch "
																		+ thisBatchId
																		+ " which may be stuck in '"
																		+ locked
																		+ "' state. If so, unlock the back using `node unlockBatch.js <batch ID>`, delete the processed file marker with `node processedFiles.js -d <filename>`, and then re-store the file in S3";
														console.log(e);
														exports.sendSNS(config.failureTopicARN.S,
																"Lambda Vertica Loader unable to write to Open Pending Batch", e, function() {
																	context.done(error, e);
																}, function(err) {
																	console.log(err);
																	context.done(error, "Unable to Send SNS Notification");
																});
													} else {
														// the add of the file was successful, so we
														exports.linkProcessedFileToBatch(itemEntry, thisBatchId);
														// which is async, so may fail but we'll still sweep
														// the pending batch
														exports.processPendingBatch(config, thisBatchId, s3Info);
													}
												}
											}
										});
					};

			/**
			 * Function which will link the deduplication table entry for the file to
			 * the batch into which the file was finally added
			 */
			exports.linkProcessedFileToBatch = function(itemEntry, batchId) {
				var updateProcessedFile = {
					Key : {
						loadFile : {
							S : itemEntry
						}
					},
					TableName : filesTable,
					AttributeUpdates : {
						batchId : {
							Action : 'PUT',
							Value : {
								S : batchId
							}
						}
					}
				};
				dynamoDB.updateItem(updateProcessedFile, function(err, data) {
					// because this is an async call which doesn't affect
					// process flow, we'll just log the error and do nothing with the OK
					// response
					if (err) {
						console.log(err);
					}
				});
			};

			/**
			 * Function to process the current pending batch, and create a batch load
			 * process if required on the basis of size or timeout
			 */
			exports.processPendingBatch =
					function(config, thisBatchId, s3Info) {
						// make the request for the current batch
						var currentBatchRequest = {
							Key : {
								batchId : {
									S : thisBatchId
								},
								s3Prefix : {
									S : s3Info.prefix
								}
							},
							TableName : batchTable,
							ConsistentRead : true
						};

						dynamoDB.getItem(currentBatchRequest,
								function(err, data) {
									if (err) {
										console.log(err);
										context.done(error, err);
									} else if (!data || !data.Item) {
										var msg = "No open pending Batch " + thisBatchId;
										console.log(msg);
										context.done(null, msg);
									} else {
										// check whether the current batch is bigger than the
										// configured max size, or older than configured max age
										var lastUpdateTime = data.Item.lastUpdate.N;
										var pendingEntries = data.Item.entries.SS;
										var doProcessBatch = false;
										if (pendingEntries.length >= parseInt(config.batchSize.N)) {
											console.log("Batch Size " + config.batchSize.N + " reached");
											doProcessBatch = true;
										}

										if (config.batchTimeoutSecs && config.batchTimeoutSecs.N) {
											if (common.now() - lastUpdateTime > parseInt(config.batchTimeoutSecs.N)
													&& pendingEntries.length > 0) {
												console.log("Batch Size " + config.batchSize.N + " not reached but reached Age "
														+ config.batchTimeoutSecs.N + " seconds");
												doProcessBatch = true;
											}
										}

										if (doProcessBatch) {
											// set the current batch to locked status
											var updateCurrentBatchStatus = {
												Key : {
													batchId : {
														S : thisBatchId,
													},
													s3Prefix : {
														S : s3Info.prefix
													}
												},
												TableName : batchTable,
												AttributeUpdates : {
													status : {
														Action : 'PUT',
														Value : {
															S : locked
														}
													},
													lastUpdate : {
														Action : 'PUT',
														Value : {
															N : '' + common.now()
														}
													}
												},
												/*
												 * the batch to be processed has to be 'open', otherwise
												 * we'll have multiple processes all handling a single
												 * batch
												 */
												Expected : {
													status : {
														AttributeValueList : [ {
															S : open
														} ],
														ComparisonOperator : 'EQ'
													}
												},
												/*
												 * add the ALL_NEW return values so we have the most up
												 * to date version of the entries string set
												 */
												ReturnValues : "ALL_NEW"
											};
											dynamoDB.updateItem(updateCurrentBatchStatus, function(err, data) {
												if (err) {
													if (err.code === conditionCheckFailed) {
														/*
														 * some other Lambda function has locked the batch -
														 * this is OK and we'll just exit quietly
														 */
														context.done(null, null);
													} else {
														console.log("Unable to lock Batch " + thisBatchId);
														context.done(error, err);
													}
												} else {
													if (!data.Attributes) {
														var e = "Unable to extract latest pending entries set from Locked batch";
														console.log(e);
														context.done(error, e);
													} else {
														/*
														 * grab the pending entries from the locked batch
														 */
														pendingEntries = data.Attributes.entries.SS;

														/*
														 * assign the loaded configuration a new batch ID
														 */
														var allocateNewBatchRequest = {
															Key : {
																s3Prefix : {
																	S : s3Info.prefix
																}
															},
															TableName : configTable,
															AttributeUpdates : {
																currentBatch : {
																	Action : 'PUT',
																	Value : {
																		S : uuid.v4()
																	}
																},
																lastBatchRotation : {
																	Action : 'PUT',
																	Value : {
																		N : '' + common.now()
																	}
																}
															}
														};

														dynamoDB.updateItem(allocateNewBatchRequest, function(err, data) {
															if (err) {
																console.log("Error while allocating new Pending Batch ID");
																console.log(err);
																context.done(error, err);
															} else {
																// OK - let's create the load config
																exports.createLoadConfig(config, thisBatchId, s3Info, pendingEntries);
															}
														});
													}
												}
											});
										} else {
											console.log("No pending batch flush required");
											context.done(null, null);
										}
									}
								});
					};

			/**
			 * Function which will create the load configuration for a given batch and entries
			 */
			exports.createLoadConfig =
					function(config, thisBatchId, s3Info, batchEntries) {
						console.log("Creating Load configuration for Batch " + thisBatchId);

						// create list of file paths for Vertica COPY
						var copyPathList = "";

						for (var i = 0; i < batchEntries.length; i++) {
							// copyPath used for Vertica loads - S3 bucket must be mounted on cluster servers 
							// as: serverS3BucketMountDir/<bucketname> (see constants.js)
							var copyPathItem = "'" + config.s3MountDir.S + batchEntries[i].replace('+', ' ').replace('%2B', '+') + "'";
							if (!copyPathList) {
                                                                copyPathList = copyPathItem;
							} else {
								copyPathList += ', ' + copyPathItem;
							}
						}
						exports.loadVertica(config, thisBatchId, s3Info, copyPathList);
					};

			/**
			 * Function run to invoke loading
			 */
			exports.loadVertica = function(config, thisBatchId, s3Info, copyPathList) {
				// convert the config.loadClusters list into a format that
				// looks like a native dynamo entry
				clustersToLoad = [];
				for (var i = 0; i < config.loadClusters.L.length; i++) {
					clustersToLoad[clustersToLoad.length] = config.loadClusters.L[i].M;
				}

				console.log("Loading " + clustersToLoad.length + " Clusters");

				// run all the cluster loaders in parallel
				async.map(clustersToLoad, function(item, callback) {
					// call the load cluster function, passing it the
					// continuation callback
					exports.loadCluster(config, thisBatchId, s3Info, copyPathList, item, callback);
				}, function(err, results) {
					if (err) {
						console.log(err);
					}

					// go through all the results - if they were all OK,
					// then close the batch OK - otherwise fail
					var allOK = true;
					var loadState = {};
					var loadStatements = {};

					for (var i = 0; i < results.length; i++) {
						if (!results[i] || results[i].status === ERROR) {
							var allOK = false;
							
							console.log("Cluster Load Failure " + results[i].error + " on Cluster " + results[i].cluster);
						} 
						// log the response state for each cluster
						loadState[results[i].cluster] = {
							status : results[i].status,
							error : results[i].error
						};
                                                       loadStatements[results[i].cluster] = {
                                                               preLoadStmt : results[i].preLoadStmt,
                                                               loadStmt : results[i].loadStmt,
                                                               postLoadStmt : results[i].postLoadStmt
                                                       };
					}

					var loadStateRequest = {
						Key : {
							batchId : {
								S : thisBatchId,
							},
							s3Prefix : {
								S : s3Info.prefix
							}
						},
						TableName : batchTable,
						AttributeUpdates : {
							clusterLoadStatus : {
								Action : 'PUT',
								Value : {
									S : JSON.stringify(loadState)
								}
							},
                                                               clusterLoadStatements : {
                                                                       Action : 'PUT',
                                                                       Value : {
                                                                               S : JSON.stringify(loadStatements)
                                                                       }
                                                               },
							lastUpdate : {
								Action : 'PUT',
								Value : {
									N : '' + common.now()
								}
							}
						}
					};
					dynamoDB.updateItem(loadStateRequest, function(err, data) {
						if (err) {
							console.log("Error while attaching per-Cluster Load State");
							exports.failBatch(err, config, thisBatchId, s3Info, loadStatements);
						} else {
							if (allOK === true) {
								// close the batch as OK
								exports.closeBatch(null, config, thisBatchId, s3Info, loadStatements);
							} else {
								// close the batch as failure
								exports.failBatch(loadState, config, thisBatchId, s3Info, loadStatements);
							}
						}
					});
				});
			};

			/**
			 * Function which loads a Vertica cluster
			 * 
			 */
			exports.loadCluster =
					function(config, thisBatchId, s3Info, copyPathList, clusterInfo, callback) {
					
						/* build the Vertica copy command */
						var copyCommand = '';
						// decrypt the encrypted items
						var encryptedItems = [ kmsCrypto.stringToBuffer(clusterInfo.connectPassword.S) ];
						kmsCrypto.decryptAll(encryptedItems, function(err, decryptedConfigItems) {
							if (err) {
								callback(err, {
									status : ERROR,
									cluster : clusterInfo.clusterEndpoint.S
								});
							} else {
								copyCommand = copyCommand + 'COPY ' + clusterInfo.targetTable.S + ' from ' + copyPathList   

								// add optional copy options
								if (config.copyOptions !== undefined) {
									copyCommand = copyCommand + ' ' + config.copyOptions.S + '\n';
								}


								// build the connection string
								console.log("Connecting to Vertica Database " + clusterInfo.clusterEndpoint.S + ":" + clusterInfo.clusterPort.N);
								var dbConnectArgs = {
									host: clusterInfo.clusterEndpoint.S,
									port: clusterInfo.clusterPort.N,
									user: clusterInfo.connectUser.S,
									password: decryptedConfigItems[0].toString(),
								} ;
								/*
								 * connect to database and run the copy command set
								 */
								var client = vertica.connect(
										dbConnectArgs,
										function(err, client, done) {
									if (err) {
										callback(null, {
											status : ERROR,
											error : err,
											cluster : clusterInfo.clusterEndpoint.S
										});
									} else {
										console.log("Connected") ;
										var preLoad = "" ;
										var load = "" ;
										var postLoad = "" ;
										// Run preLoad Statement, if defined - failure will not affect batch state
										if (clusterInfo.preLoadStatement !== undefined) {
											var statement = clusterInfo.preLoadStatement.S ;
											console.log("Execute preLoadStatement: " + statement) ;
											client.query(statement, function(err, result) {
												if (err) {
													console.log("preLoadStatement: Failed");
													preLoad = "Failed: " + statement ;
												} else {
													console.log("preLoadStatement: Success");
													preLoad = "Success: " + statement ;
												}
											}) ;
										}
										// Run Load statement					
										console.log("Execute load statement: " + copyCommand) ;
										client.query(copyCommand, function(err, result) {
											// handle errors and cleanup
											if (err) {
												console.log("Load: Failed");
                                                                                                load = "Failed: " + copyCommand ;
												callback(null, {
													status : ERROR,
													error : err,
													preLoadStmt : preLoad,
													loadStmt : load,
													postLoadStmt : postLoad,
													cluster : clusterInfo.clusterEndpoint.S
												});
											} else {
												console.log("Load: Success");
                                                                                                load = "Success: " + copyCommand ;
												// Run postLoad Statement, if defined
												if (clusterInfo.postLoadStatement !== undefined) {
													var statement = clusterInfo.postLoadStatement.S ;
													console.log("Execute postLoadStatement: " + statement) ;
													client.query(statement, function(err, result) {	
														if (err) {
															console.log("postLoadStatement: Failed");
                                                                                                        		postLoad = "Failed: " + statement ;
															callback(null, {
																status : ERROR,
			                                                                                                        error : err,
																preLoadStmt : preLoad,
																loadStmt : load,
																postLoadStmt : postLoad,
                                                                                                        			cluster : clusterInfo.clusterEndpoint.S
															});
														} else {
															console.log("postLoadStatement: Success");
                                                                                                        		postLoad = "Success: " + statement ;
															callback(null, {
																status : OK,
																error : null,
																preLoadStmt : preLoad,
																loadStmt : load,
																postLoadStmt : postLoad,
																cluster : clusterInfo.clusterEndpoint.S
															});
														}
													}) ;
												} else { 
													callback(null, {
														status : OK,
														error : null,
														preLoadStmt : preLoad,
														loadStmt : load,
														postLoadStmt : postLoad,
														cluster : clusterInfo.clusterEndpoint.S
													});
												}
											}
										});
									}
								});
							}
						});
					};

			/**
			 * Function which marks a batch as failed and sends notifications
			 * accordingly
			 * Original version handled failed manifest copies - this code has bene removed, so function is no a no-op.
			 */
			exports.failBatch = function(loadState, config, thisBatchId, s3Info, loadStatements) {
				console.log('Batch failed.');
				exports.closeBatch(loadState, config, thisBatchId, s3Info, loadStatements);
				};

			/**
			 * Function which closes the batch to mark it as done, including
			 * notifications
			 */
			exports.closeBatch = function(batchError, config, thisBatchId, s3Info, loadStatements) {
				var batchEndStatus;

				if (batchError && batchError !== null) {
					batchEndStatus = error;
				} else {
					batchEndStatus = complete;
				}

				var item = {
					Key : {
						batchId : {
							S : thisBatchId
						},
						s3Prefix : {
							S : s3Info.prefix
						}
					},
					TableName : batchTable,
					AttributeUpdates : {
						status : {
							Action : 'PUT',
							Value : {
								S : batchEndStatus
							}
						},
						lastUpdate : {
							Action : 'PUT',
							Value : {
								N : '' + common.now()
							}
						}
					}
				};

				// add the error message to the updates if we had one
				if (batchError && batchError !== null) {
					item.AttributeUpdates.errorMessage = {
						Action : 'PUT',
						Value : {
							S : JSON.stringify(batchError)
						}
					};
				}

				// mark the batch as closed
				dynamoDB.updateItem(item, function(err, data) {
					// ugh, the batch closure didn't finish - this is not a good
					// place to be
					if (err) {
						console.log(err);
						context.done(error, err);
					} else {
						// send notifications
						exports.notify(config, thisBatchId, s3Info, batchError, loadStatements);
					}
				});
			};

			/** send an SNS message to a topic */
			exports.sendSNS = function(topic, subj, msg, successCallback, failureCallback) {
				var m = {
					Message : JSON.stringify(msg),
					Subject : subj,
					TopicArn : topic
				};

				sns.publish(m, function(err, data) {
					if (err) {
						if (failureCallback) {
							failureCallback(err);
						} else {
							console.log(err);
						}
					} else {
						if (successCallback) {
							successCallback();
						}
					}
				});
			};

			/** Send SNS notifications if configured for OK vs Failed status */
			exports.notify =
					function(config, thisBatchId, s3Info, batchError, loadStatements) {
						var statusMessage = batchError ? 'error' : 'ok';
						var errorMessage = batchError ? JSON.stringify(batchError) : null;
						var messageBody = {
							error : errorMessage,
							status : statusMessage,
							batchId : thisBatchId,
							s3Prefix : s3Info.prefix
						};

						if (loadStatements) {
							messageBody.loadStatements = loadStatements
						}

						if (batchError && batchError !== null) {
							console.log(JSON.stringify(batchError));

							if (config.failureTopicARN) {
								exports.sendSNS(config.failureTopicARN.S, "Lambda Vertica Batch Load " + thisBatchId + " Failure",
										messageBody, function() {
											context.done(error, JSON.stringify(batchError));
										}, function(err) {
											console.log(err);
											context.done(error, err);
										});
							} else {
								context.done(error, batchError);
							}
						} else {
							if (config.successTopicARN) {
								exports.sendSNS(config.successTopicARN.S, "Lambda Vertica Batch Load " + thisBatchId + " OK",
										messageBody, function() {
											context.done(null, null);
										}, function(err) {
											console.log(err);
											context.done(error, err);
										});
							} else {
								// finished OK - no SNS notifications for
								// success
								console.log("Batch Load " + thisBatchId + " Complete");
								context.done(null, null);
							}
						}
					};
			/* end of runtime functions */

			// commented out event logger, for debugging if needed
			// console.log(JSON.stringify(event));
					
			if (!event.Records) {
				// filter out unsupported events
				console.log("Event type unsupported by Lambda Vertica Loader");
				console.log(JSON.stringify(event));
				context.done(null, null);
			} else {
				if (event.Records.length > 1) {
					context.done(error, "Unable to process multi-record events");
				} else {
					for (var i = 0; i < event.Records.length; i++) {
						var r = event.Records[i];

						// ensure that we can process this event based on a variety
						// of criteria
						var noProcessReason = undefined;
						if (r.eventSource !== "aws:s3") {
							noProcessReason = "Invalid Event Source " + r.eventSource;
						}
						if (!(r.eventName === "ObjectCreated:Copy" || r.eventName === "ObjectCreated:Put" || r.eventName === 'ObjectCreated:CompleteMultipartUpload')) {
							noProcessReason = "Invalid Event Name " + r.eventName;
						}
						if (r.s3.s3SchemaVersion !== "1.0") {
							noProcessReason = "Unknown S3 Schema Version " + r.s3.s3SchemaVersion;
						}

						if (noProcessReason) {
							console.log(noProcessReason);
							context.done(error, noProcessReason);
						} else {
							// extract the s3 details from the event
							var inputInfo = {
								bucket : undefined,
								key : undefined,
								prefix : undefined,
								inputFilename : undefined
							};

							inputInfo.bucket = r.s3.bucket.name;
							inputInfo.key = decodeURIComponent(r.s3.object.key);

							// remove the bucket name from the key, if we have
							// received it
							// - happens on object copy
							inputInfo.key = inputInfo.key.replace(inputInfo.bucket + "/", "");

							var keyComponents = inputInfo.key.split('/');
							inputInfo.inputFilename = keyComponents[keyComponents.length - 1];

							// remove the filename from the prefix value
							var searchKey = inputInfo.key.replace(inputInfo.inputFilename, '').replace(/\/$/, '');

							// if the event didn't have a prefix, and is just in the
							// bucket, then just use the bucket name, otherwise add the prefix
							if (searchKey && searchKey !== null && searchKey !== "") {
								var regex = /(=\d+)+/;
								// transform hive style dynamic prefixes into static
								// match prefixes
								do {
									searchKey = searchKey.replace(regex, "=*");
								} while (searchKey.match(regex) !== null);

								searchKey = "/" + searchKey;
							}
							inputInfo.prefix = inputInfo.bucket + searchKey;

							// load the configuration for this prefix
							var dynamoLookup = {
								Key : {
									s3Prefix : {
										S : inputInfo.prefix
									}
								},
								TableName : configTable,
								ConsistentRead : true
							};

							var proceed = false;
							var lookupConfigTries = 10;
							var tryNumber = 0;
							var configData = null;

							async.whilst(function() {
								// return OK if the proceed flag has been set, or if
								// we've hit the retry count
								return !proceed && tryNumber < lookupConfigTries;
							}, function(callback) {
								tryNumber++;

								// lookup the configuration item, and run
								// foundConfig on completion
								dynamoDB.getItem(dynamoLookup, function(err, data) {
									if (err) {
										if (err.code === provisionedThroughputExceeded) {
											// sleep for bounded jitter time up to 1
											// second and then retry
											var timeout = common.randomInt(0, 1000);
											console.log(provisionedThroughputExceeded + " while accessing Configuration. Retrying in "
													+ timeout + " ms");
											setTimeout(callback(), timeout);
										} else {
											// some other error - call the error callback
											callback(err);
										}
									} else {
										configData = data;
										proceed = true;
										callback(null);
									}
								});
							}, function(err) {
								if (err) {
									// fail the context as we haven't been able to
									// lookup the onfiguration
									console.log(err);
									context.done(error, err);
								} else {
									// call the foundConfig method with the data item
									exports.foundConfig(inputInfo, null, configData);
								}
							});
						}
					}
				}
			}
		};

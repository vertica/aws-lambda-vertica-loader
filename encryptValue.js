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


require('./constants');

var region = process.argv[2];
var input = process.argv[3];
var kmsCrypto = require('./kmsCrypto');

if (!input) {
	console.log("You must provide a region for the KMS Service and an input value to Encrypt");
	process.exit(ERROR);
} else {
	kmsCrypto.setRegion(region);
	kmsCrypto.encrypt(input, function(err, encryptedCiphertext) {
		if (err) {
			console.log(err);
			process.exit(ERROR);
		} else {
			kmsCrypto.decrypt(encryptedCiphertext, function(err, plaintext) {
				if (err) {
					console.log(err);
					process.exit(ERROR);
				} else {
					if (plaintext.toString() === input) {
						console.log("Encryption completed and verified with AWS KMS");

						console.log(JSON.stringify({
							inputValue : input,
							configurationEntryValue : kmsCrypto.toLambdaStringFormat(encryptedCiphertext)
						}));
					} else {
						console.log("Encryption completed but could not be verified");
						process.exit(ERROR);
					}
				}
			});
		}
	});
}

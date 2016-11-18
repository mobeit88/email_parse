var AWS = require('aws-sdk');
AWS.config.update({
	region : "us-west-2",
});

var s3 = new AWS.S3();
var MailParser = require("mailparser").MailParser;
var request = require('request');

//
// parse the attachment using the balemail translate api
//
var post_email_data = function(email_json, cb) {
	console.log("post_email_data star...");
	post_data = {
		"flatLines" : email_json.attachments[0].content.toString()
	};

	console.log(post_data);
	request({
		method : "POST",
		headers : [ {
			name : 'content-type',
			value : 'application/json'
		} ],

		//url : 'http://balemail.com/api/DataHandler/flatToJson',
		url : 'https://b7w5c119rh.execute-api.us-west-2.amazonaws.com/development/flattojson',
		json : post_data
	}, function(err, httpResponse, body) {
		if (err) {
			console.error('upload failed:', err);
			cb(err, httpResponse);
		}
		console.log('Upload successful! Server responded with:', body);
		cb(err, body);
	});
};

//
// upload file to a given bucket/key ("bucket/key" = "folder/filename")
//
var upload_file = function(bucket, key, json_data, cb) {
	console.log("file json_data: ");
	console.log(json_data);
	s3.putObject({
		Bucket : bucket,
		Key : key,
		// ACL : 'public-read',
		Body : JSON.stringify(json_data),
		ContentType : "application/json"
	}, function(err, data) {
		console.log(err);
		console.log(data);
		cb(err, data);
	});
};

//
// the main function to be called by Lambda. 
// 
exports.handler = function(event, context, callback) {
	console.log('Process email start');
	console.log(JSON.stringify(event));

	// the bucket containing the incoming email to be parsed into a message
//	var inboxBucket = 'balemail-inbox-email'; // 'balemail-inbox';
	// the bucket to receive the parsed messages
	var bucketName = 'balemail.info-process-messages';//'balemail-process-messages'; //'balemail-data'; // balemail.info-inbox';

	var notification = event.Records[0].s3;
	var notificationKey = notification.object.key;
	//var sesNotification = event.Records[0].ses;
	// console.log("SES Notification:\n", JSON.stringify(sesNotification, null,
	// 2));


console.log("notificationKey: ", notificationKey);
//console.log("inboxBucket: ", inboxBucket);


	// Retrieve the email from your bucket
s3.getObject({
        Bucket: notification.bucket.name, //inboxBucket,
		Key : notificationKey
	}, function(err, data) {
		if (err) {
			console.log(err, err.stack);
			callback(err);
		} else {
			console.log("Raw email:\n" + data.Body);
			var mailparser = new MailParser();
			// Custom email processing goes here

			// setup an event listener when the parsing finishes
			mailparser.on("end", function(mail_object) {
				console.log("parsed email: ");
				console.log(mail_object);
				mail_json = mail_object;
				// mail_json.to = mail_object.to;
				// mail_json.cc = mail_object.cc;
				// mail_json.bcc = mail_object.bcc;
				// mail_json.from = mail_object.from;
				// mail_json.subject = mail_object.subject;
				// mail_json.html = mail_object.html;
				// mail_json.date = mail_object.date;
				// mail_json.attachments = mail_object.attachments;
				console.log((mail_json));

//				key_name = mail_json.from[0].address + "/" + notificationKey + ".json";

//				console.log("key_name:" + key_name);

// TODO: loop through mail_json.to array and a) add userid for all emails we have in our system, and b) create copies of all of those in bucketName bucket
				// console.log(mail_json.attachments[0].content.toString());
				if (mail_json.attachments != undefined && mail_json.attachments.length > 0) {
					post_email_data(mail_json, function(err_email, email_post_data) {
						if (err_email) {

						} else {
							mail_json.post_data = email_post_data;
						}
					});
				}

				// upload email into balemail.info-process-messages bucket
				upload_file(bucketName, notificationKey + ".json", mail_json, function(err_upload, data_upload) {
					callback(null, null);
				});
				
				// adds email addresses to array
				var addrList = [];
				for(var i in mail_json.to) {
					addrList.push(mail_json.to[i]);
				}
				for(var i in mail_json.cc) {
					addrList.push(mail_json.cc[i]);
				}
				// currently not working for bcc
//				for(var i in mail_json.bcc) {
//					addrList.push(mail_json.bcc[i]);
//				}

				var tempCredentials = AWS.config.credentials;

				for(var i in addrList) {
					// assume role for DynamoDB access
					AWS.config.credentials = new AWS.TemporaryCredentials({
						RoleArn: 'arn:aws:iam::908307179958:role/StageAlpha',
					});

					var docClient = new AWS.DynamoDB.DocumentClient();

					var table = "BaleMailUsers";

					var user_email = addrList[i]["address"];

					var params = {
						TableName: table,
						FilterExpression : 'Email = :this_email',
						ExpressionAttributeValues : { ':this_email' : user_email }
					};

					docClient.scan(params, function(err, dbdata) {
						if (err) {
							console.error("Unable to read item. Error JSON:", JSON.stringify(err, null, 2));
						} else {
							console.log("GetItem succeeded:", JSON.stringify(dbdata, null, 2));

							if (dbdata["Items"].length > 0) {
								// upload email JSON into balemail.info-data bucket
								upload_file('balemail.info-data', dbdata["Items"][0]["UserId"] + "/" + notificationKey + ".json", mail_json, function(err_upload, data_upload) {
									callback(null, null);
								});
							}
						}
						AWS.config.credentials = tempCredentials;
					});				
				}
			});

			// send the email source to the parser
			mailparser.write(data.Body);
			mailparser.end();

		}
	});

};

// lambda-local -l index.js -h handler -e data/input.js -t 60
// node-lambda run

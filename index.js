'use strict';

const AWS = require('aws-sdk');
const fs = require('fs');
const date = require('date-and-time');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const request = require('request');
const csv = require('csvtojson');
const queryString = require('query-string');
const regex = new RegExp('-','g');
var start = new Date('2018-12-01');
var end = new Date('2018-12-31');

function padZero(num){
	if (!(num<10)){
		return num;
	}
	return '0'+num;
}   

exports.handler = async (event, context) => {
   

    const s3 = new AWS.S3();

    const downloadDir = '/tmp/downloads/';
    const outputDir = '/tmp/output/';

    if (!fs.existsSync(downloadDir)) {
        fs.mkdirSync(downloadDir);
    }
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
    }

    const funcPromise = async () => {
        return new Promise( (resolve, reject) => {
            let today = new Date();
            let from = today.getMonth()+1+'/'+(today.getDate())+'/'+today.getFullYear();
            //let from = '12/4/2019';
            let fromParts = from.split('/');
            let formatedFrom = fromParts[2] + '-' + padZero(fromParts[0]) + '-' + padZero(fromParts[1]);
            let timeslots = [];
            let inputDate = new Date(fromParts[2], padZero(fromParts[0]) - 1, padZero(fromParts[1]));
            let dayAfterInputDate = date.addDays(inputDate, 1);
            var nextDayString = dayAfterInputDate.getFullYear() + '-' + padZero(dayAfterInputDate.getMonth() + 1) + '-' + padZero(dayAfterInputDate.getDate());
            for (var i = 0; i < 25; i++) {
                var suffix = ':00:00';
                var hour = i;
                if (i < 10) {
                    hour = '0' + i.toString();
                }
                if (i === 24) {
                    timeslots.push(nextDayString + ' ' + '00' + suffix);
                } else {
                    timeslots.push(formatedFrom + ' ' + hour + suffix);
                }
            }
           
            var outputObjArr = [];

            const bus_num = {'156108': '1618', '156183': '1614', '160707': '2350'};

            let arrayPromise = [];

            request.get('https://cpm.checkpt.com/web/(S(oiaxdg3wgnlareiakaeo3p02))/login/login.aspx', function (err, res, body) {
                let redirectedAddress = res.request.uri.href;
                const viewState = '/wEPDwULLTE0NTExOTE3MTEPZBYCAgUPZBYIAgUPZBYIAgEPDxYGHgVXaWR0aBsAAAAAAIBhQAEAAAAeCU1heExlbmd0aAIyHgRfIVNCAoACZGQCAg8PFgIeDEVycm9yTWVzc2FnZQUUVXNlck5hbWUgaXMgcmVxdWlyZWRkZAIEDxYCHgRUZXh0BQNZZXNkAgcPFgIfBAUFMTQwcHhkAgcPZBYIAgEPDxYIHwICgAIeBE1vZGULKiVTeXN0ZW0uV2ViLlVJLldlYkNvbnRyb2xzLlRleHRCb3hNb2RlAh8BAjIfABsAAAAAAIBhQAEAAABkZAICDw8WAh8DBRRQYXNzd29yZCBpcyByZXF1aXJlZGRkAgQPFgIfBAUDWWVzZAIHDxYCHwQFBTE0MHB4ZAILD2QWCmYPDxYCHghJbWFnZVVybAUqfi9pbWFnZXMvYm9yZGVycy9MZWZ0U2lkZV9Cb3JkZXJfV2hpdGUuZ2lmZGQCAQ8PFgYeCENzc0NsYXNzBQ1idXR0b25SQ1doaXRlHwAbAAAAAACAW0ABAAAAHwICggJkFgJmDw8WAh8EBQVMb2dpbmRkAgIPDxYCHwYFK34vaW1hZ2VzL2JvcmRlcnMvUmlnaHRTaWRlX0JvcmRlcl9XaGl0ZS5naWZkZAIDDxYCHwQFGExvZ2luQnV0dG9uUkNfbGJsQ29udGVudGQCBQ8WAh8EBQUxMTBweGQCDQ9kFgJmD2QWAmYPZBYCZg8QZA8WCmYCAQICAgMCBAIFAgYCBwIIAgkWChAFB0NoaW5lc2UFBDEwMjhnEAUFRHV0Y2gFBDEwNDNnEAUHRW5nbGlzaAUEMTAzM2cQBQZGcmVuY2gFBDEwMzZnEAUGR2VybWFuBQQxMDMxZxAFB0l0YWxpYW4FBDEwNDBnEAUISmFwYW5lc2UFBDEwNDFnEAUGUG9saXNoBQQxMDQ1ZxAFB1J1c3NpYW4FBDEwNDlnEAUHU3BhbmlzaAUEMzA4MmcWAQICZBgBBR5fX0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WAwUQU2F2ZVVOUFdDaGVja2JveAUeTG9naW5CdXR0b25SQyRidXR0b25SQ0xlZnRQYXJ0BR9Mb2dpbkJ1dHRvblJDJGJ1dHRvblJDUmlnaHRQYXJ0PdDXdZrWVNMOwTr8lJXye/0WvdZ+KGEjEvOQobjlu8k=';
                const eventValidation = '/wEdABLyXB1T63gmwFtpw0QtYZnmEjNTGW8v+hLXC+FiMZwyvDiXaBxSkAecv70aBmEQga1lWe+byVY7YSIlff9Uq4b64XX9GB1cyJ7Pus7A1hclG5C0Of8x94PqYH7U2NbDVk776HriIl95T6N9qh1+BnxUaxCv4xeih55ev401fbdocl3tw2WZo9137z7Arq38gTEgaLrigwbeogKnq5IofoHqo04F2ZNpwVC7RHKDjcuYQ+kjsB3OD2iQP4LFdjLFl8bHSlqaUsohmXqdZs1up1pspPk/HvwczGt1LM01dhNbYxdn6LP5h7mV5NqjdWFysFRMeM60F2YfhDiahjPLGlpC3IH3zsRsQWGQRH5dN/Fr/G9nskwm2NY61xNuA+P24zU84UZ00azyvqb7rm08/KS1vRIeaIFSjuVxB4xCNiacKA==';
                let loginFormData = {
                    __EVENTTARGET: 'LoginButtonRC$buttonRCContent',
                    __EVENTARGUMENT: '',
                    __LASTFOCUS: '',
                    __VIEWSTATE: viewState,
                    __VIEWSTATEGENERATOR: '96AE78F2',
                    __EVENTVALIDATION: eventValidation,
                    UsernameTextBox$textBoxRC: 'decathlonpc',
                    PasswordTextBox$textBoxRC: 'Shekou416',
                    LoginButtonRC: 'LoginButtonRC',
                    ddlLanguageM: 1033
                };

                request.post({
                    url: redirectedAddress,
                    formData: loginFormData
                }, (err, res, body) => {
                    let responseHeaders = res.headers['set-cookie'][1];
                    let ckpAuth = responseHeaders.split(';')[0].replace('CkpAuth=', '');
                    let baseUri = res.request.uri.href;
                    baseUri = baseUri.replace('/login/login.aspx', '');
                    const viewUrlSuffix = '/reporting/viewer.aspx?';
                    const headerReferSuffix = '/reporting/parameters.aspx?prod=1';
                    const locationId = ['156108', '156183', '160707'];

                    for (let i = 0; i < locationId.length; i++) {
                        let requestParams = {
                            rid: "85",
                            pLocationId: locationId[i],
                            pDateRangeFlag: "True",
                            pDateFrom: from,
                            pDateTo: from,
                            pLangId: "1033",
                            pReportName: 'traffic summary by business hour',
                            action: 'DownloadCSV'
                        };

                        let viewerReqHeaders = {
                            Cookie: "language=1033; CkpAuth=" + ckpAuth,
                            Host: "cpm.checkpt.com",
                            Referer: baseUri + headerReferSuffix + queryString.stringify(requestParams)
                        };

                        let options = {
                            url: baseUri + viewUrlSuffix,
                            method: 'GET',
                            headers: viewerReqHeaders,
                            qs: requestParams
                        };

                        let filename = 'trafficReport_' + locationId[i] + '_' + formatedFrom.replace('-', '') + '.csv';

                        //TODO : load on s3
                        let file = fs.createWriteStream(downloadDir + filename);
                        arrayPromise.push(new Promise(function (resolve, reject) {
                            let piping = request(options).pipe(file);
                            piping.on('finish', () => {
                                resolve();
                            })
                        }));
                    }

                    Promise.all(arrayPromise).then(() => {
                        let outputPromiseArr = [];
                        fs.readdir(downloadDir, function (err, files) {
                            if (err) {
                                process.exit(1);
                            }
                            files.forEach((file, index) => {
                                let fileOutPutPromise = new Promise((resolve, reject) => {
                                    csv().fromFile(downloadDir + file).then((jsonArray) => {
                                        var fileNameParts = file.split('_');
                                        var busNumKey = fileNameParts[1].replace('.csv', '');
                                        //var totalProcessed = false;
                                        for (let i = 0; i < jsonArray.length; i++) {
                                            if (jsonArray[i].Level !== 'Door') {
                                                continue;
                                            }
                                            const dataStart = 79;
                                            //const totalStart = 132;
                                            let row = Object.values(jsonArray[i]);
                                            let rowData = row.slice(79,128);
                                            let figure = [];
                                            for(var rowDataCount=0;rowDataCount<rowData.length;rowDataCount++){
                                                if (!( rowData[rowDataCount] === '')){
                                                    figure.push(rowData[rowDataCount]);
                                                }
                                                continue;
                                            }
                                            
                                            if(figure.length === 0) {
                                                continue;
                                            }
                                            let timeslotCount = 0;
                                            for (let j = 1; j <= 48; j += 2) {
                                                let outputObj = {};
                                                outputObj.bus_unit = bus_num[busNumKey];
                                                outputObj.door = row[dataStart];
                                                outputObj.in = row[dataStart + j].replace(',','');
                                                outputObj.out = row[dataStart + j + 1].replace(',' ,'');
                                                outputObj.datetime_start = timeslots[timeslotCount];
                                                outputObj.datetime_end = timeslots[timeslotCount + 1];
                                                outputObjArr.push(outputObj);
                                                timeslotCount++;
                                            }
                                        }
                                        resolve(outputObjArr);
                                    });
                                });
                                outputPromiseArr.push(fileOutPutPromise);
                            });

                            Promise.all(outputPromiseArr).then(function (values) {
                                let outputFileName = 'traffic_data_' + formatedFrom.replace(regex, '') + '.csv';
                                const csvWriter = createCsvWriter({
                                    path: outputDir + outputFileName,
                                    header: [
                                        {id: 'bus_unit', title: 'but_num_business_unit'},
                                        {id: 'door', title: 'door'},
                                        {id: 'datetime_start', title: 'datetime_start'},
                                        {id: 'datetime_end', title: 'datetime_end'},
                                        {id: 'in', title: 'in'},
                                        {id: 'out', title: 'out'}
                                    ]
                                });

                                csvWriter.writeRecords(values[1]).then(async () => {
                                    const fileContent = fs.readFileSync(outputDir + outputFileName);
                                    const params = {
                                        Bucket: 'store-traffic-data',
                                        Key: '2019/'+outputFileName,
                                        Body: fileContent
                                    };
                                    try{
                                        await s3.upload(params).promise();
                                        resolve();
                                    }
                                    catch (e) {
                                        reject(e);
                                    }
                                });
                            });
                        });
                    });
                });
            });
        });
    };

    return funcPromise();
};

/**
 * Created by rakib on 8/19/15.
 */

var RESPONSE_TIMEOUT_IN_SECONDS = 5;
var request = require('request').defaults({
        timeout: RESPONSE_TIMEOUT_IN_SECONDS * 1000
    }),
    cheerio = require('cheerio'),
    rp = require('request-promise-native'),
    async = require('async');

var SCRAPOXY_PROXY = '';
var SCRAPOXY_UI = '';
var SCRAPOXY_MAXIMUM_INSTANCE_COUNT = 40; // this should be same as set in scrapoxy configs, 120 seconds recommended
var SCRAPOXY_PASSWORD = 'samerz';

// Cases when instances will be restarted
var ON_FIRST_ERROR = true; // restart on first blocking error

var EVERY_N_MINUTES = true; // restart every N minute
var RESTART_INTERVAL_MINUTES = 20; // restart every 20 min

var EVERY_N_REQUEST = true; // restart every N request
var REQUESTS_COUNT = 1500;

var WAIT_AFTER_RESTARTING_SECONDS = 120 // Time to get ready new instances

var REQUEST_DELAY_IN_SECONDS = 2;

function getInstanceList(callback) {
    var opts = {
        method: 'GET',
        url: `${SCRAPOXY_UI}/api/instances`,
        headers: {
            'Authorization': new Buffer(SCRAPOXY_PASSWORD).toString('base64'),
        },
        json: true
    };

    rp(opts).then(instances => {
        callback(null, instances || []);
    }).catch(callback);
}

function stopInstance(name, callback) {
    var opts = {
        method: 'POST',
        url: `${SCRAPOXY_UI}/api/instances/stop`,
        headers: {
            'Authorization': new Buffer(SCRAPOXY_PASSWORD).toString('base64'),
        },
        json: {
            name: name
        }
    };

    rp(opts).then(() => {
        callback(null);
    }).catch(callback);
}

function restartInstances(callback) {
    console.log('Getting list of instances...');
    getInstanceList(function (err, instances) {
        if (err) {
            return callback(err);
        }

        async.map(instances, function (instance, nextInstance) {
            console.log(`Restarting instance ${instance.address.hostname}...`)
            stopInstance(instance.name, function () {
                nextInstance();
            });
        }, function () {
            console.log('Waiting 120 seconds to initilize new instances...');
            setTimeout(function () {
                callback();
                console.log('All instances restarted');
            }, WAIT_AFTER_RESTARTING_SECONDS * 1000);
        });
    });
}


module.exports = {
    crawl: function (crawlTask, crawlCities, done) {
        var alreadyRestarting = false;
        var requestsCount = 0;
        var intervalInstance;
        if (EVERY_N_MINUTES) {
            intervalInstance = setInterval(function () {
                if (alreadyRestarting) {
                    return;
                }
                console.log(`Restarting instances reason: Restart every ${RESTART_INTERVAL_MINUTES} minutes `);
                alreadyRestarting = true;
                queue.pause();
                restartInstances(function () {
                    alreadyRestarting = false;
                    queue.resume();
                });
            }, RESTART_INTERVAL_MINUTES * 60 * 1000);
        }

        var nCities = crawlCities.length;
        var queue = async.queue(function (task, callback) {

            setTimeout(function () {

                doTask(task, callback)

            }, REQUEST_DELAY_IN_SECONDS * 1000);

        }, SCRAPOXY_MAXIMUM_INSTANCE_COUNT);

        function doTask(task, callback) {
            request({url: task.url, proxy: SCRAPOXY_PROXY}, function (err, response, body) {
                requestsCount ++;
                if (!response) {
                    return callback({code: -1, task: task})
                }

                if (response.statusCode != 200) {
                    return callback({code: -2, message: "BLOCKED"});
                }

                var newCars = [];
                var $ = cheerio.load(body);
                $('div').each(function (i, el) {
                    if ($(el).attr('class') == 'content') {
                        var content = cheerio.load($(el).html());
                        content('li').each(function (j, elem) {
                            if (content(elem).attr('class') == 'result-row') {
                                var id = content(elem).attr('data-pid')
                                    , row = cheerio.load(content(elem).html())
                                    , title = row('.hdrlnk').text()
                                    , price = row('span .result-price').text()
                                    , owner = row('small').text()
                                    , date = row('.result-date').text()
                                    , link = row('a').attr('href');

                                if (link.indexOf("craigslist.org") === -1) {
                                    link = "https://" + task.domain + link;
                                }
                                else {
                                    if (!link.startsWith("http")) {
                                        if (link.startsWith("//")) {
                                            link = "https:" + link;
                                        }
                                        else {
                                            link = "https://" + link
                                        }
                                    }
                                }

                                var newCar = {
                                    taskId: task.taskId,
                                    carId: id,
                                    date: date,
                                    owner: owner,
                                    link: link,
                                    price: price,
                                    title: title
                                };
                                newCars.push(newCar)
                            }
                        })
                    } // END if
                });

                console.log("Found ", newCars.length, "cars. GOOD");
                callback(null, newCars, task.domain);
            });
        }

        function afterTask(err, newCars) {
            if (err && err.code == -2) {
                nCities--;
                if (nCities == 0) {
                    return done(err);
                }
                console.log('Found  0 cars: BLOCKED');
                if (ON_FIRST_ERROR && !alreadyRestarting) {
                    console.log('Restarting instances reason: Restart on first blocking ');
                    alreadyRestarting = true;
                    queue.pause();
                    restartInstances(function () {
                        alreadyRestarting = false;
                        queue.push(err.task, afterTask);
                        queue.resume();
                    });
                }
                return;
            }

            if (err) {
                queue.push(err.task, afterTask);
                return;
            }

            nCities--;
            if (nCities == 0) {
                return done();
            }

            if (EVERY_N_REQUEST && requestsCount >= REQUESTS_COUNT && !alreadyRestarting) {
                console.log(`Restarting instances reason: Restart every ${REQUESTS_COUNT} requests `);
                alreadyRestarting = true;
                requestsCount = 0;
                queue.pause();
                restartInstances(function () {
                    alreadyRestarting = false;
                    queue.resume();
                });
            }

            var cars = [];
            async.eachSeries(newCars, function (car, cb) {
                Car.create(car, function (err, c) {
                    if (!err && c) {
                        cars.push(c);
                        Car.publishCreate(c);
                    }
                    cb();
                })
            }, function () {
                if (cars.length > 0) {
                    NotifierService.sendEmail(cars, "bzeaiter@gmail.com");
                }
            })

        }

        crawlCities = crawlCities || [];
        crawlCities.forEach(function (city) {
            if (!city) {
                return;
            }

            var temp = city + ".craigslist.org";
            queue.push({
                url: "https://" + temp + crawlTask.path,
                domain: temp,
                taskId: crawlTask.taskId
            }, afterTask);
        });

        queue.drain = function () {
            console.log('Crawling completed.');
            clearInterval(intervalInstance);
        };
    }
};



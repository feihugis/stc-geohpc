/**
 * Created by feihu on 4/24/16.
 */

var merra2gifs = [], x = -1;
var refreshIntervalId;

googledrive = "https://googledrive.com/host/0B8as2mZ2Oud6aklmR3lIclFjYTg/EVAP198001"

for (i = 0; i< 31; i++) {
    if (i<10) {
        merra2gifs[i] = googledrive + 0 + i + ".gif";
    } else {
        merra2gifs[i] = googledrive + i + ".gif";
    }
}
/*
merra2gifs[0] = "http://localhost:8080/gif/EVAP19800101.gif"
merra2gifs[1] = "http://localhost:8080/gif/EVAP19800102.gif"
merra2gifs[2] = "http://localhost:8080/gif/EVAP19800103.gif"
merra2gifs[3] = "http://localhost:8080/gif/EVAP19800104.gif"
merra2gifs[4] = "http://localhost:8080/gif/EVAP19800105.gif"
merra2gifs[5] = "http://localhost:8080/gif/EVAP19800106.gif"
merra2gifs[6] = "http://localhost:8080/gif/EVAP19800107.gif"
merra2gifs[7] = "http://localhost:8080/gif/EVAP19800108.gif"
merra2gifs[8] = "http://localhost:8080/gif/EVAP19800109.gif"
merra2gifs[9] = "http://localhost:8080/gif/EVAP19800110.gif"*/

function displayNextImage() {
    x = (x === merra2gifs.length - 1) ? 0 : x + 1;
    $("#merra2gif").attr("src", merra2gifs[x]);
}

function displayPreviousImage() {
    x = (x <= 0) ? merra2gifs.length - 1 : x - 1;
    $("#merra2gif").attr("src", merra2gifs[x]);
}

function startTimer() {
    x = -1;
    refreshIntervalId = setInterval(displayNextImage, 500*24);
}

function stopTimer() {
    clearInterval(refreshIntervalId);
}

function queryMerra2() {
    var varName = $('#varName').val();
    urlParams = {
        vars: varName,
        startTime: "19800101",
        endTime: "20150101",
        statename: "Alaska,Hawaii,Puerto",
        isObject: "false",
        isGlobal: "false"
    }

    $.get("query/merra2", urlParams, function(data, status) {
        merra2gifs = data;
        startTimer();

        //$("#merra2gif").attr("src", "http://localhost:8080/gif/EVAP19800101.gif");
    })
}

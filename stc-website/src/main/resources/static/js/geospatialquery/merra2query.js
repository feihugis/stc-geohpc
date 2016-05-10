/**
 * Created by feihu on 4/24/16.
 */

var merra2gifs = [], x = -1;
var refreshIntervalId;

googledrive = "https://googledrive.com/host/0B8as2mZ2Oud6aklmR3lIclFjYTg/EVAP198001"

for (i = 1; i< 31; i++) {
    if (i<10) {
        merra2gifs[i] = googledrive + 0 + i + ".gif";
    } else {
        merra2gifs[i] = googledrive + i + ".gif";
    }
}

var map;
var slectedStates = []
AmCharts.ready( function() {
    map = new AmCharts.AmMap();
    map.panEventsEnabled = true;
    map.backgroundColor = "#666666";
    map.backgroundAlpha = 1;

    var dataProvider = {
        map: "usaLow",
        getAreasFromMap: true
    };

    map.dataProvider = dataProvider;

    map.areasSettings = {
        autoZoom: false,
        color: "#CDCDCD",
        colorSolid: "#5EB7DE",
        selectedColor: "#5EB7DE",
        outlineColor: "#666666",
        rollOverColor: "#88CAE7",
        rollOverOutlineColor: "#FFFFFF",
        selectable: true
    };

    map.addListener( 'clickMapObject', function( event ) {
        // deselect the area by assigning all of the dataProvider as selected object
        map.selectedObject = map.dataProvider;

        // toggle showAsSelected
        event.mapObject.showAsSelected = !event.mapObject.showAsSelected;

        // bring it to an appropriate color
        map.returnInitialColor( event.mapObject );

        // let's build a list of currently selected states
        var states = [];
        for ( var i in map.dataProvider.areas ) {
            var area = map.dataProvider.areas[ i ];
            if ( area.showAsSelected ) {
                states.push( area.title );
                slectedStates.push( area.title );
            }
        }
    } );
    map.export = {
        enabled: true
    }

    map.write( "chartdiv" );
} );


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
        vars: "EVAP",
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

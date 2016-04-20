L.mapbox.accessToken = 'pk.eyJ1IjoiZGl0dGEiLCJhIjoiai1IR2ltbyJ9.Cdx7m5BlRn1uXYZODfnp5A';
var map = L.mapbox.map('map', 'mapbox.streets')
    .setView([37.897, -96.034], 5);
//var markerLayer = L.mapbox.featureLayer().addTo(map);

$.ajax({
    url: '/getAllPhotos',
    type: 'GET',
    dateType: 'json',
    success: loadFlickr
       });

var photoCount = 0;
var min_lat = 0.0, min_lon = 0.0, max_lat = 0.0, max_lon = 0.0;

var markers = new L.MarkerClusterGroup();

function loadFlickr(response) {
    photoCount = 0;
    min_lat = 0.0;
    min_lon = 0.0;
    max_lat = 0.0;
    max_lon = 0.0;

    markers.clearLayers();


    $.each(response.photoFlickrList, function(i, item) {
        photoCount++;

        if(min_lat > item.lat && item.lat < 90 && item.lat > -90) {
            min_lat = item.lat;
        }

        if(max_lat < item.lat && item.lat < 90 && item.lat > -90) {
            max_lat = item.lat;
        }

        if(min_lon > item.lon) {
            min_lon = item.lon;
        }

        if(max_lon < item.lon) {
            max_lon = item.lon;
        }

        var slideshowContent = '<div class="image' + ' active' + '">' +
                            '<img src="' + item.url + '" />' +
                            '<a href="' + item.userlink + '" target="_blank">'+
                            '<div class="caption"> User Link </div> </a>' +
                            '</div>';

        var popupContent =  '<div class="popup">' +
                            '<h2>' + item.title + '</h2>' +
                            '<div class="slideshow">' +
                            slideshowContent +
                            '</div>' + '</div>';

        var marker = L.marker(new L.LatLng(item.lat, item.lon), {
                icon: L.mapbox.marker.icon({
                                               'marker-color': '#9c89cc',
                                               'marker-size': 'small'
                                           })
            })
            .bindPopup(popupContent, {
                closeButton: false,
                minWidth: 320
            });

        markers.addLayer(marker);
    });

    map.addLayer(markers);
    //markers.addTo(markerLayer);

    $('.infoControl').remove();
    infoControl.addTo(map);
    $('#number').html("Photo Number : " + photoCount);
    $('#min_lat').html("Min Lat : " + min_lat.toFixed(4));
    $('#min_lon').html("Min Lon : " + min_lon.toFixed(4));
    $('#max_lat').html("Max Lat : " + max_lat.toFixed(4));
    $('#max_lon').html("Max Lon : " + max_lon.toFixed(4));

}

// adding the search window in the topright corner
myControl = L.control({
                          position: 'topright'
                      });

myControl.onAdd = function(map) {
    this._div = L.DomUtil.create('div', 'myControl');
    this._div.innerHTML = '<h1 id="title">Flickr Photo Map</h1>' +
                          '<input type="text" id="myTextField" value="search by key..." />' +
                          '<input type="submit" id="searchBtn" value="Search" onclick="fetchPhotoByKey()"/>';
    L.DomEvent.disableClickPropagation(this._div);
    return this._div;
}
myControl.addTo(map);

//Functions to either disable (onmouseover) or enable (onmouseout) the map's dragging
function controlEnter(e) {
    map.dragging.disable();
}

function controlLeave() {
    map.dragging.enable();
}

//Quick application to all input tags
var inputTags = document.getElementsByTagName("input")
for (var i = 0; i < inputTags.length; i++) {
    inputTags[i].onmouseover = controlEnter;
    inputTags[i].onmouseout = controlLeave;
}

infoControl = L.control({
                            position: 'topright'
                        });

infoControl.onAdd = function(map) {
    this._div = L.DomUtil.create('div', 'infoControl');
    this._div.innerHTML =  '<div>'
                            + '<div id="infobg">' + '</div>'
                            + ' <div id="info">'
                                  + '<div id="number"> Photo Number : ' + photoCount + '</div>'
                                  + '<div id="min_lat"> Min Lat : ' + min_lat.toFixed(4) + '</div>'
                                  + '<div id="min_lon"> Min Lon : ' + min_lon.toFixed(4) + '</div>'
                                  + '<div id="max_lat"> Max Lat : ' + max_lat.toFixed(4) + '</div>'
                                  + '<div id="max_lon"> Mat Lon : ' + max_lon.toFixed(4) + '</div>'
                           + '</div>'
                           + '</div>';
    L.DomEvent.disableClickPropagation(this._div);
    return this._div;
}

function fetchPhotoByKey() {
    $('.infoControl').remove();
    infoControl.addTo(map);
    $('#number').html("Photo Number : " + 0);
    $('#min_lat').html("Min Lat : " + 0);
    $('#min_lon').html("Min Lon : " + 0);
    $('#max_lat').html("Max Lat : " + 0);
    $('#max_lon').html("Max Lon : " + 0);
    var query = document.getElementById('myTextField').value;

    if (query.length == 0) {
        alert('Nothing to search for...');
        return;
    }

    //$('.leaflet-marker-pane').empty();



    $.ajax({
               url: '/getPhotosByKey?key=' + query,
               type: 'GET',
               dateType: 'json',
               success: loadFlickr
           });
}

$('#map').on('click', '.trigger', function(e) {
    //alert('Hello from Toronto!');
    var tooltip = new mapboxgl.Popup()
        .setLngLat(e.lngLat)
        .setHTML('<h1>Hello World!</h1>')
        .addTo(map);
});
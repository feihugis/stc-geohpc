/**
 * At the beginning, we convert the Json to GeoJson, then add them as markers into the feature layer.
 * But this process is not efficient, because Mapbox can directly use Json to create markers. Thus, we
 * do not use this javascript now.
 */

L.mapbox.accessToken = 'pk.eyJ1IjoiZGl0dGEiLCJhIjoiai1IR2ltbyJ9.Cdx7m5BlRn1uXYZODfnp5A';
var map = L.mapbox.map('map', 'mapbox.streets')
                  .setView([38.897, -77.034], 14);
var markerLayer = L.mapbox.featureLayer().addTo(map);

$.ajax({
           url: '/getAllPhotos',
           type: 'GET',
           dateType: 'json',
           success: loadFlickr
       });

function loadFlickr(response) {
    var geoJson = [];
    markerLayer.setGeoJSON(geoJson);

    $.each(response.photoFlickrList, function(i, item) {
        var feature = {
            type: 'Feature',
            "geometry": { "type": "Point", "coordinates": [item.lon, item.lat]},
            "properties": {
                'title': item.title,
                'marker-color': '#9c89cc',
                'marker-size': 'small',
                'images': [
                    [item.url,item.userlink]
                ]
            }
        };

        geoJson.push(feature);
    });

    markerLayer.on('layeradd', function(e) {
        var marker = e.layer;
        var feature = marker.feature;
        var images = feature.properties.images
        var slideshowContent = '';

        for(var i = 0; i < images.length; i++) {
            var img = images[i];

            slideshowContent += '<div class="image' + (i === 0 ? ' active' : '') + '">' +
                                '<img src="' + img[0] + '" />' +
                                '<a href="' + img[1] + '" target="_blank">'+
                                '<div class="caption"> User Link </div> </a>' +
                                '</div>';
        }

        // Create custom popup content
        var popupContent =  '<div id="' + feature.properties.id + '" class="popup">' +
                            '<h2>' + feature.properties.title + '</h2>' +
                            '<div class="slideshow">' +
                            slideshowContent +
                            '</div>' + '</div>';

        // http://leafletjs.com/reference.html#popup
        marker.bindPopup(popupContent,{
            closeButton: false,
            minWidth: 320
        });
    });

    markerLayer.setGeoJSON(geoJson);
}

// adding the search window in the bottomleft corner
myControl = L.control({
                          position: 'topright'
                      });

myControl.onAdd = function(map) {
    this._div = L.DomUtil.create('div', 'myControl');
    this._div.innerHTML = '<h1 id="title">Flickr Photo Map</h1>' +
                          '<input type="text" id="myTextField" value="search by key..." />' +
                          '<input type="submit" id="searchBtn" value="Search" onclick="fetchPhotoByKey()"/>'
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


function fetchPhotoByKey() {
    var query = document.getElementById('myTextField').value;

    if (query.length == 0) {
        alert('Nothing to search for...');
        return;
    }
   // $('.leaflet-marker-pane').empty();

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
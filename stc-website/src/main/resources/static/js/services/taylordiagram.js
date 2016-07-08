/**
 * Created by feihu on 5/8/16.
 */

var startTime, endTime, minLat, minLon, maxLat, maxLon, isCMAP, isGPCP, isMerra2, isMerra1, isCFSR, isERAINTRIM;
var referenceModel = "CMAP";

function getTaylordiagram() {
    //var varName = $('#varName').val();
    startTime = $('#start_time').val();
    endTime = $('#end_time').val();
    minLat = $('#min_lat').val();
    minLon = $('#min_lon').val();
    maxLat = $('#max_lat').val();
    maxLon = $('#max_lon').val();
    isCMAP = $("#isCMAP").is(":checked")
    isGPCP = $("#isGPCP").is(":checked")
    isMerra2 = $("#iSMERRA2").is(":checked")
    isMerra1 = $("#isMERRA1").is(":checked")
    isCFSR = $("#isCFSR").is(":checked")
    isERAINTRIM = $("#isERAINTRIM").is(":checked")

    urlParams = {
        start_time: startTime,
        end_time: endTime,
        min_lat: minLat,
        max_lat: maxLat,
        min_lon: minLon,
        max_lon: maxLon,
        isCMAP: isCMAP,
        isGPCP: isGPCP,
        isMERRA2: isMerra2,
        isMERRA1: isMerra1,
        isCFSR: isCFSR,
        isERAINTRIM: isERAINTRIM,
        referenceModel: referenceModel
    }

    $.get("query/taylordiagram", urlParams, function(data, status) {
        taylordiagram_uri = data;
        //$("#taylordiagram").attr("src", taylordiagram_uri);
        var time_range = startTime + " - " + endTime;
        var space_range = "(" + minLat + "," + minLon + ")" + " - " + " (" + maxLat + "," + maxLon + ")";
        var model_list = "CMAP"

        if (isGPCP) {model_list = model_list + ", GPCP"}
        if (isMerra2) {model_list = model_list + ", MERRA2"}
        if (isMerra1) {model_list = model_list + ", MERRA1"}
        if (isCFSR) {model_list = model_list + ", CFSR"}
        if (isERAINTRIM) {model_list = model_list + ", ERAINTRIM"}

        addTaylorDiagramDiv(taylordiagram_uri, time_range, space_range, model_list);
    })
}

function deleteDiv() {
    $(this).parent().parent().remove();
}

function addTaylorDiagramDiv(taylordiagram_uri, time_range, space_range, model_list) {
    var tmp = $("#taylordiagram-panel").clone();
    tmp.find("#taylordiagram").attr("src", taylordiagram_uri);
    tmp.find("#time_range").html(time_range);
    tmp.find("#space_range").html(space_range);
    tmp.find("#model_list").html(model_list);
    $(".container-fluid").append("<div class='row' id='taylordiagram-panel'>" + tmp.html() + "</div>")
}


$(".dropdown-menu li a").click(function(){
    referenceModel = $(this).text();
    $(this).parents().find('#reference-model').text($(this).text());
    $(this).parents().find('#reference-model').val($(this).text());
    $(this).parents().find('#reference-model').append("<span class='caret'></span>")
});
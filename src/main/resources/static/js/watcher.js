(function () {
    window.watcher = function () {
        var lastImpression = null;
        var currentImpression = null;

        function getInitial() {
            $.getJSON("/service/dsp_event/latest", function (data) {
                if (data.impressions.length != 0) {
                    currentImpression = data.impressions[0];
                    updateDisplay();
                }
            });
        }

        function getUpdate() {
            $.getJSON("/service/dsp_event/" + currentImpression.impressionId, function (data) {
                if (data.impressions.length != 0) {
                    lastImpression = currentImpression;
                    currentImpression = data.impressions[0];
                    updateDisplay();
                }
            });
        }

        function next() {
            $.getJSON("/service/dsp_event/" + currentImpression.impressionId + "/next", function (data) {
                if (data.impressions.length != 0) {
                    lastImpression = currentImpression;
                    currentImpression = data.impressions[0];
                    updateDisplay();
                }
            });
        }

        function previous() {
            $.getJSON("/service/dsp_event/" + currentImpression.impressionId + "/previous", function (data) {
                if (data.impressions.length != 0) {
                    lastImpression = currentImpression;
                    currentImpression = data.impressions[0];
                    updateDisplay();
                }
            });
        }

        function getMore() {
            $.getJSON("/service/dsp_event/" + currentImpression.impressionId + "/more", function (data) {
                if (data.more) {
                    $("#forward").show();
                }
                else {
                    $("#forward").hide();
                }
            });
        }

        function getColumns(events) {
            var columns = [];
            for (var i = 0; i < events.length; i++) {
                var event = events[i];
                var ownPropertyNames = Object.getOwnPropertyNames(event);
                for (var j = 0; j < ownPropertyNames.length; j++) {
                    var propName = ownPropertyNames[j];
                    if (columns.indexOf(propName) === -1) {
                        columns.push(propName)
                    }
                }
            }
            return columns;
        }

        var dodgyEvents = ["INVALID_AD_DIMENSION_RESPONSE", "INVALID_CAMPAIGN_CLASSIFICATION_RESPONSE", "INVALID_CAMPAIGN_BRAND_CATEGORY_RESPONSE",
            "INVALID_FORMAT_RESPONSE", "INVALID_CAMPAIGN_AGENCY_RESPONSE", "TIMEOUT", "MALFORMED_RESPONSE", "BAD_HTTP_RESPONSE_CODE", "UNKNOWN_ERROR",
            "UNQUALIFIED_BID", "BLACKLIST_CHECK", "REFERRER_CHECK_BLOCK", "BID_BELOW_FLOOR", "BRAND_ON_BID_NOT_ALLOWED_BY_DEAL", "DEAL_ID_MISMATCH",
            "SEAT_MISMATCH", "APPROVAL_STATUS_DROPPED", "APPROVAL_STATUS_PENDING", "APPROVAL_STATUS_DECLINED", "APPROVAL_STATUS_BLOCKED"];

        function isDodgy(event) {
            return dodgyEvents.indexOf(event.eventType) !== -1;
        }

        function updateDisplay() {
            if (!_.isEqual(lastImpression, currentImpression)) {
                var thing = $("#currentImpression");
                thing.empty();
                var events = currentImpression.events;
                var columns = getColumns(events);
                var table = $("<table class='table'>");
                var header = $("<tr>");
                for (var i = 0; i < columns.length; i++) {
                    var head = $("<th>");
                    head.text(columns[i]);
                    header.append(head);
                }
                table.append(header);
                for (var j = 0; j < events.length; j++) {
                    var row = $("<tr>");
                    var event = events[j];
                    if (isDodgy(event)) {
                        row.addClass("danger");
                    }
                    else {
                        row.addClass("success");
                    }
                    for (var k = 0; k < columns.length; k++) {
                        var col = $("<td>");
                        col.text(event[columns[k]]);
                        row.append(col);
                    }
                    table.append(row);
                }
                thing.append(table);
                getMore();
            }
        }

        function update() {
            if (currentImpression == null) {
                getInitial();
            }
            else {
                getUpdate();
            }
        }

        function more() {
            if (currentImpression != null) {
                getMore();
            }
        }

        setInterval(update, 1000);
        setInterval(more, 1000);
        $("#forward").click(next);
        $("#back").click(previous);
    }
})();
$(window.watcher);
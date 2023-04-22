// Database fields to query
const FIELDS = [
    "ID",
    "Severity",
    "Start_Time",
    "End_Time",
    "Start_Lat",
    "Start_Lng",
    "End_Lat",
    "End_Lng",
    "Distance",
    "Number",
    "Street",
    "Side",
    "City",
    "County",
    "State",
    "Zipcode",
    "Country",
    "Timezone",
    "Airport_Code",
    "Weather_Timestamp",
    "Temperature",
    "Wind_Chill",
    "Humidity",
    "Pressure",
    "Visibility",
    "Wind_Direction",
    "Wind_Speed",
    "Precipitation",
    "Weather_Condition",
    "Amenity",
    "Bump",
    "Crossing",
    "Give_Way",
    "Junction",
    "No_Exit",
    "Railway",
    "Roundabout",
    "Station",
    "Stop",
    "Traffic_Calming",
    "Traffic_Signal",
    "Turning_Loop",
    "Sunrise_Sunset",
    "Civil_Twilight",
    "Nautical_Twilight",
    "Astronomical_Twilight"
];

/**
 * Sends an HTTP request to the server and receives data. The data is then shown as an HTML table.
 * @param {Array<string>} fields Database fields or columns to query
 */
async function getData(fields) {
    // Adds fields as query params.
    const params = new URLSearchParams()
    fields.forEach(field => params.append("fields", field))

    // Sends HTTP request and parses JSON response.
    const response = await fetch(`/accidents?${params}`);
    const data = await response.json();
    console.log(data);

    // Displays received data in table.
    const container = document.getElementById("accidents-table");
    const rows = data.map(record => htmlJoin(Object.values(record), "td"));
    const header = data.length > 0 ? Object.keys(data[0]) : [];
    container.innerHTML = htmlJoin(header, "th") + htmlJoin(rows, "tr");
}

/**
 * Joins a list of HTML strings wrapping them in tags.
 * @param {Array<string>} data List HTML strings to join.
 * @param {string} separator Tag used to wrap each HTML string.
 */
function htmlJoin(data, separator) {
    const tag = {
        start: `<${separator}>`,
        end: `</${separator}>`
    };

    return data.map(element => `${tag.start}${element}${tag.end}`).join("");
}

getData(FIELDS);
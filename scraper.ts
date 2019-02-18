// Parses the development applications at the South Australian District Council of Grant web site
// and places them in a database.
//
// Michael Bone
// 16th February 2019

"use strict";

import * as fs from "fs";
import * as cheerio from "cheerio";
import * as request from "request-promise-native";
import * as sqlite3 from "sqlite3";
import * as urlparser from "url";
import * as moment from "moment";
import * as pdfjs from "pdfjs-dist";
import didYouMean, * as didyoumean from "didyoumean2";

sqlite3.verbose();

const DevelopmentApplicationsUrl = "https://www.dcgrant.sa.gov.au/developmentregister";
const CommentUrl = "mailto:info@dcgrant.sa.gov.au";

declare const process: any;

// Address information.

let StreetNames = null;
let StreetSuffixes  = null;
let SuburbNames = null;
let HundredSuburbNames = null;

// Sets up an sqlite database.

async function initializeDatabase() {
    return new Promise((resolve, reject) => {
        let database = new sqlite3.Database("data.sqlite");
        database.serialize(() => {
            database.run("create table if not exists [data] ([council_reference] text primary key, [address] text, [description] text, [info_url] text, [comment_url] text, [date_scraped] text, [date_received] text, [legal_description] text)");
            resolve(database);
        });
    });
}

// Inserts a row in the database if the row does not already exist.

async function insertRow(database, developmentApplication) {
    return new Promise((resolve, reject) => {
        let sqlStatement = database.prepare("insert or ignore into [data] values (?, ?, ?, ?, ?, ?, ?, ?)");
        sqlStatement.run([
            developmentApplication.applicationNumber,
            developmentApplication.address,
            developmentApplication.description,
            developmentApplication.informationUrl,
            developmentApplication.commentUrl,
            developmentApplication.scrapeDate,
            developmentApplication.receivedDate,
            developmentApplication.legalDescription
        ], function(error, row) {
            if (error) {
                console.error(error);
                reject(error);
            } else {
                if (this.changes > 0)
                    console.log(`    Inserted: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\", description \"${developmentApplication.description}\", legal description \"${developmentApplication.legalDescription}\" and received date \"${developmentApplication.receivedDate}\" into the database.`);
                else
                    console.log(`    Skipped: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\", description \"${developmentApplication.description}\", legal description \"${developmentApplication.legalDescription}\" and received date \"${developmentApplication.receivedDate}\" because it was already present in the database.`);
                sqlStatement.finalize();  // releases any locks
                resolve(row);
            }
        });
    });
}

// A 2D point.

interface Point {
    x: number,
    y: number
}

// A bounding rectangle.

interface Rectangle {
    x: number,
    y: number,
    width: number,
    height: number
}

// An element (consisting of text and intersecting cells) in a PDF document.

interface Element extends Rectangle {
    text: string
}

// A cell in a grid (owning zero, one or more elements).

interface Cell extends Rectangle {
    elements: Element[]
}

// Reads all the address information into global objects.

function readAddressInformation() {
    // Read the street names.

    StreetNames = {}
    for (let line of fs.readFileSync("streetnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetNameTokens = line.toUpperCase().split(",");
        let streetName = streetNameTokens[0].trim();
        let suburbName = streetNameTokens[1].trim();
        (StreetNames[streetName] || (StreetNames[streetName] = [])).push(suburbName);  // several suburbs may exist for the same street name
    }

    // Read the street suffixes.

    StreetSuffixes = {};
    for (let line of fs.readFileSync("streetsuffixes.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetSuffixTokens = line.toUpperCase().split(",");
        StreetSuffixes[streetSuffixTokens[0].trim()] = streetSuffixTokens[1].trim();
    }

    // Read the suburb names and hundred names.

    SuburbNames = {};
    HundredSuburbNames = {};
    for (let line of fs.readFileSync("suburbnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let suburbTokens = line.toUpperCase().split(",");
        
        let suburbName = suburbTokens[0].trim();
        SuburbNames[suburbName] = suburbTokens[1].trim();
        if (suburbName.startsWith("MOUNT ")) {
            SuburbNames["MT " + suburbName.substring("MOUNT ".length)] = suburbTokens[1].trim();
            SuburbNames["MT." + suburbName.substring("MOUNT ".length)] = suburbTokens[1].trim();
            SuburbNames["MT. " + suburbName.substring("MOUNT ".length)] = suburbTokens[1].trim();
        }

        for (let hundredName of suburbTokens[2].split(";")) {
            hundredName = hundredName.trim();
            (HundredSuburbNames[hundredName] || (HundredSuburbNames[hundredName] = [])).push(suburbName);  // several suburbs may exist for the same hundred name
            if (hundredName.startsWith("MOUNT ")) {
                let mountHundredName = "MT " + hundredName.substring("MOUNT ".length);
                (HundredSuburbNames[mountHundredName] || (HundredSuburbNames[mountHundredName] = [])).push(suburbName);  // several suburbs may exist for the same hundred name
                mountHundredName = "MT." + hundredName.substring("MOUNT ".length);
                (HundredSuburbNames[mountHundredName] || (HundredSuburbNames[mountHundredName] = [])).push(suburbName);  // several suburbs may exist for the same hundred name
                mountHundredName = "MT. " + hundredName.substring("MOUNT ".length);
                (HundredSuburbNames[mountHundredName] || (HundredSuburbNames[mountHundredName] = [])).push(suburbName);  // several suburbs may exist for the same hundred name
            }
        }
    }
}

// Constructs a rectangle based on the intersection of the two specified rectangles.

function intersect(rectangle1: Rectangle, rectangle2: Rectangle): Rectangle {
    let x1 = Math.max(rectangle1.x, rectangle2.x);
    let y1 = Math.max(rectangle1.y, rectangle2.y);
    let x2 = Math.min(rectangle1.x + rectangle1.width, rectangle2.x + rectangle2.width);
    let y2 = Math.min(rectangle1.y + rectangle1.height, rectangle2.y + rectangle2.height);
    if (x2 >= x1 && y2 >= y1)
        return { x: x1, y: y1, width: x2 - x1, height: y2 - y1 };
    else
        return { x: 0, y: 0, width: 0, height: 0 };
}

// Calculates the fraction of an element that lies within a cell (as a percentage).  For example,
// if a quarter of the specifed element lies within the specified cell then this would return 25.

function getPercentageOfElementInCell(element: Element, cell: Cell) {
    let elementArea = getArea(element);
    let intersectionArea = getArea(intersect(cell, element));
    return (elementArea === 0) ? 0 : ((intersectionArea * 100) / elementArea);
}

// Calculates the area of a rectangle.

function getArea(rectangle: Rectangle) {
    return rectangle.width * rectangle.height;
}

// Gets the percentage of horizontal overlap between two rectangles (0 means no overlap and 100
// means 100% overlap).

function getHorizontalOverlapPercentage(rectangle1: Rectangle, rectangle2: Rectangle) {
    if (rectangle1 === undefined || rectangle2 === undefined)
        return 0;

    let startX1 = rectangle1.x;
    let endX1 = rectangle1.x + rectangle1.width;

    let startX2 = rectangle2.x;
    let endX2 = rectangle2.x + rectangle2.width;

    if (startX1 >= endX2 || endX1 <= startX2 || rectangle1.width === 0 || rectangle2.width === 0)
        return 0;

    let intersectionWidth = Math.min(endX1, endX2) - Math.max(startX1, startX2);
    let unionWidth = Math.max(endX1, endX2) - Math.min(startX1, startX2);

    return (intersectionWidth * 100) / unionWidth;
}

// Formats the text as a street.  If the text is not recognised as a street then undefined is
// returned.

function formatStreet(text: string) {
    if (text === undefined)
        return undefined;

    let tokens = text.trim().toUpperCase().split(" ");

    // Parse the street suffix (this recognises both "ST" and "STREET").

    let token = tokens.pop();
    let streetSuffix = StreetSuffixes[token];
    if (streetSuffix === undefined)
        streetSuffix = Object.values(StreetSuffixes).find(streetSuffix => streetSuffix === token);

    // The text is not considered to be a valid street if it has no street suffix.

    if (streetSuffix === undefined)
        return undefined;

    // Add back the expanded street suffix (for example, this converts "ST" to "STREET").

    tokens.push(streetSuffix);

    // Extract tokens from the end of the array until a valid street name is encountered (this
    // looks for an exact match).

    for (let index = 4; index >= 2; index--) {
        let suburbNames = StreetNames[tokens.slice(-index).join(" ")];
        if (suburbNames !== undefined)
            return { streetName: tokens.join(" "), suburbNames: suburbNames };  // reconstruct the street with the leading house number (and any other prefix text)
    }

    // Extract tokens from the end of the array until a valid street name is encountered (this
    // allows for a spelling error).

    for (let index = 4; index >= 2; index--) {
        let streetNameMatch = <string>didYouMean(tokens.slice(-index).join(" "), Object.keys(StreetNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 1, trimSpaces: true });
        if (streetNameMatch !== null) {
            let suburbNames = StreetNames[streetNameMatch];
            tokens.splice(-index, index);  // remove elements from the end of the array           
            return { streetName: (tokens.join(" ") + " " + streetNameMatch).trim(), suburbNames: suburbNames };  // reconstruct the street with the leading house number (and any other prefix text)
        }
    }

    return undefined;
}

// Formats the address, ensuring that it has a valid suburb, state and post code.

function formatAddress(address: string) {
    // Allow for a few special cases (ie. road type suffixes and multiple addresses).

    address = address.replace(/ TCE NTH/g, " TERRACE NORTH").replace(/ TCE STH/g, " TERRACE SOUTH").replace(/ TCE EAST/g, " TERRACE EAST").replace(/ TCE WEST/g, " TERRACE WEST");

    // Break the address up based on commas (the main components of the address are almost always
    // separated by commas).

    let tokens = address.split(",");

    // Find the location of the street name in the tokens.

    let streetNameIndex = 3;
    let formattedStreet = formatStreet(tokens[tokens.length - streetNameIndex]);  // the street name is most likely in the third to last token (so try this first)
    if (formattedStreet === undefined) {
        streetNameIndex = 2;
        formattedStreet = formatStreet(tokens[tokens.length - streetNameIndex]);  // try the second to last token (occasionally happens)
        if (formattedStreet === undefined) {
            streetNameIndex = 4;
            formattedStreet = formatStreet(tokens[tokens.length - streetNameIndex]);  // try the fourth to last token (rare)
            if (formattedStreet === undefined)
                return address;  // if a street name is not found then give up
        }
    }

    // If there is one token after the street name then assume that it is a hundred name.  For
    // example,
    //
    // LOT 15, SECTION P.2299,  KIRIP RD, HINDMARSH

    if (streetNameIndex === 2) {
        let hundredSuburbNames = [];

        let token = tokens[tokens.length - 1].trim();
        if (token.startsWith("HD "))
            token = token.substring("HD ".length).trim();

        let hundredNameMatch = <string>didYouMean(token, Object.keys(HundredSuburbNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 2, trimSpaces: true });
        if (hundredNameMatch !== null)
            hundredSuburbNames = HundredSuburbNames[hundredNameMatch];

        // Construct the intersection of two arrays of suburb names (ignoring the array of suburb
        // names derived from the hundred name if it is empty).

        let intersectingSuburbNames = formattedStreet.suburbNames
            .filter(suburbName => hundredSuburbNames === null || hundredSuburbNames.indexOf(suburbName) >= 0);
        let suburbName = (intersectingSuburbNames.length === 0) ? formattedStreet.suburbNames[0] : intersectingSuburbNames[0];

        // Reconstruct the full address using the formatted street name and determined suburb name.

        tokens = tokens.slice(0, tokens.length - streetNameIndex);
        tokens.push(formattedStreet.streetName);
        tokens.push(SuburbNames[suburbName]);
        return tokens.join(", ");
    }

    // If there are two tokens after the street name then assume that they are the suburb name
    // followed by the hundred name (however, if the suburb name is prefixed by "HD " then assume
    // that they are both hundred names).  For example,
    //
    // LOT 1, 2 BAKER ST, SOUTHEND, RIVOLI BAY
    // LOT 4, SECTION ,  KIRIP RD, HD HINDMARSH, HINDMARSH
    //
    // If there are three tokens after the street name then ignore the first token and assume that
    // the second and third tokens are the suburb name followed by the hundred name (however, if
    // the suburb name is prefixed by "HD " then assume that they are both hundred names).  For
    // example,
    //
    // SECTION P.399, 10 SOMERVILLE ST,S.O.T.P, BEACHPORT, RIVOLI BAY
    // LOT 4, 20 SOMERVILLE ST, S.O.T.P., HD RIVOLI BAY, RIVOLI BAY

    if (streetNameIndex === 3 || streetNameIndex === 4) {
        let hundredSuburbNames1 = [];
        let hundredSuburbNames2 = [];
        let suburbNames = [];

        let token = tokens[tokens.length - 1].trim();
        if (token.startsWith("HD "))
            token = token.substring("HD ".length).trim();

        let hundredNameMatch = <string>didYouMean(token, Object.keys(HundredSuburbNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 2, trimSpaces: true });
        if (hundredNameMatch !== null)
            hundredSuburbNames1 = HundredSuburbNames[hundredNameMatch];

        // The other token is usually a suburb name, but is sometimes a hundred name (as indicated
        // by a "HD " prefix).

        token = tokens[tokens.length - 2].trim();
        if (token.startsWith("HD ")) {
            token = token.substring("HD ".length).trim();
            let hundredNameMatch = <string>didYouMean(token, Object.keys(HundredSuburbNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 2, trimSpaces: true });
            if (hundredNameMatch !== null)
                hundredSuburbNames2 = HundredSuburbNames[hundredNameMatch];
        } else {
            let suburbNameMatch = didYouMean(token, Object.keys(SuburbNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 2, trimSpaces: true });
            if (suburbNameMatch !== null)
                suburbNames = [ suburbNameMatch ];
        }

        // Construct the intersection of all the different arrays of suburb names (ignoring any
        // arrays that are empty).

        let intersectingSuburbNames = formattedStreet.suburbNames
            .filter(suburbName => hundredSuburbNames1.length === 0 || hundredSuburbNames1.indexOf(suburbName) >= 0)
            .filter(suburbName => hundredSuburbNames2.length === 0 || hundredSuburbNames2.indexOf(suburbName) >= 0)
            .filter(suburbName => suburbNames.length === 0 || suburbNames.indexOf(suburbName) >= 0)
        let suburbName = (intersectingSuburbNames.length === 0) ? formattedStreet.suburbNames[0] : intersectingSuburbNames[0];

        // Reconstruct the full address using the formatted street name and determined suburb name.

        tokens = tokens.slice(0, tokens.length - streetNameIndex);
        tokens.push(formattedStreet.streetName);
        tokens.push(SuburbNames[suburbName]);
        return tokens.join(", ");
    }

    return address;
}

// Examines all the lines in a page of a PDF and constructs cells (ie. rectangles) based on those
// lines.

async function parseCells(page) {
    let operators = await page.getOperatorList();

    // Find the lines.  Each line is actually constructed using a rectangle with a very short
    // height or a very narrow width.

    let lines: Rectangle[] = [];

    let previousRectangle = undefined;
    let transformStack = [];
    let transform = [ 1, 0, 0, 1, 0, 0 ];
    transformStack.push(transform);

    for (let index = 0; index < operators.fnArray.length; index++) {
        let argsArray = operators.argsArray[index];

        if (operators.fnArray[index] === pdfjs.OPS.restore)
            transform = transformStack.pop();
        else if (operators.fnArray[index] === pdfjs.OPS.save)
            transformStack.push(transform);
        else if (operators.fnArray[index] === pdfjs.OPS.transform)
            transform = pdfjs.Util.transform(transform, argsArray);
        else if (operators.fnArray[index] === pdfjs.OPS.constructPath) {
            let argumentIndex = 0;
            for (let operationIndex = 0; operationIndex < argsArray[0].length; operationIndex++) {
                if (argsArray[0][operationIndex] === pdfjs.OPS.moveTo)
                    argumentIndex += 2;
                else if (argsArray[0][operationIndex] === pdfjs.OPS.lineTo)
                    argumentIndex += 2;
                else if (argsArray[0][operationIndex] === pdfjs.OPS.rectangle) {
                    let x1 = argsArray[1][argumentIndex++];
                    let y1 = argsArray[1][argumentIndex++];
                    let width = argsArray[1][argumentIndex++];
                    let height = argsArray[1][argumentIndex++];
                    let x2 = x1 + width;
                    let y2 = y1 + height;
                    [x1, y1] = pdfjs.Util.applyTransform([x1, y1], transform);
                    [x2, y2] = pdfjs.Util.applyTransform([x2, y2], transform);
                    width = x2 - x1;
                    height = y2 - y1;
                    previousRectangle = { x: x1, y: y1, width: width, height: height };
                }
            }
        } else if (operators.fnArray[index] === pdfjs.OPS.fill && previousRectangle !== undefined) {
            lines.push(previousRectangle);
            previousRectangle = undefined;
        }
    }

    // Determine all the horizontal lines and vertical lines that make up the grid.

    let horizontalLines: Rectangle[] = [];
    let verticalLines: Rectangle[] = [];

    for (let line of lines) {
        // Ignore short lines or smaller rectangles (since these are probably part of the logo at
        // the top left of the page).

        if (line.width < 200 && line.height < 200)
            continue;
        
        // Convert any larger rectangles into lines.  This almost always corresponds to a header
        // (so only take note of the horizontal lines; avoid allowing the vertical lines to cause
        // very narrow cells to be created).

        if ((line.width > 10 && line.height > 200) || (line.height > 10 && line.width > 200)) {
            horizontalLines.push({ x: line.x, y: line.y, width: line.width, height: 1 });
            horizontalLines.push({ x: line.x, y: line.y + line.height, width: line.width, height: 1 });
            continue;
        }
            
        if (line.height <= 2)  // horizontal line
            horizontalLines.push(line);
        else  // vertical line
            verticalLines.push(line);
    }

    let verticalLineComparer = (a, b) => (a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0);
    verticalLines.sort(verticalLineComparer);

    let horizontalLineComparer = (a, b) => (a.y > b.y) ? 1 : ((a.y < b.y) ? -1 : 0);
    horizontalLines.sort(horizontalLineComparer);
    
    // Construct cells based on the grid of lines.

    let cells: Cell[] = [];

    for (let horizontalLineIndex = 0; horizontalLineIndex < horizontalLines.length - 1; horizontalLineIndex++) {
        for (let verticalLineIndex = 0; verticalLineIndex < verticalLines.length - 1; verticalLineIndex++) {
            let horizontalLine = horizontalLines[horizontalLineIndex];
            let nextHorizontalLine = horizontalLines[horizontalLineIndex + 1];
            let verticalLine = verticalLines[verticalLineIndex];
            let nextVerticalLine = verticalLines[verticalLineIndex + 1];
            cells.push({ elements: [], x: verticalLine.x, y: horizontalLine.y, width: nextVerticalLine.x - verticalLine.x, height: nextHorizontalLine.y - horizontalLine.y });
        }
    }

    return cells;
}

// Parses the text elements from a page of a PDF.

async function parseElements(page) {
    let textContent = await page.getTextContent();

    // Find all the text elements.

    let elements: Element[] = textContent.items.map(item => {
        let transform = item.transform;

        // Work around the issue https://github.com/mozilla/pdf.js/issues/8276 (heights are
        // exaggerated).  The problem seems to be that the height value is too large in some
        // PDFs.  Provide an alternative, more accurate height value by using a calculation
        // based on the transform matrix.

        let workaroundHeight = Math.sqrt(transform[2] * transform[2] + transform[3] * transform[3]);

        let x = transform[4];
        let y = transform[5];
        let width = item.width;
        let height = workaroundHeight;

        return { text: item.str, x: x, y: y, width: width, height: height };
    });

    return elements;
}

// Parses a PDF document.

async function parsePdf(url: string) {
    console.log(`Reading development applications from ${url}.`);

    let developmentApplications = [];

    // Read the PDF.

    let buffer = await request({ url: url, encoding: null, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);

    // Parse the PDF.  Each page has the details of multiple applications.

    let pdf = await pdfjs.getDocument({ data: buffer, disableFontFace: true, ignoreErrors: true });
    for (let pageIndex = 0; pageIndex < pdf.numPages; pageIndex++) {
        console.log(`Reading and parsing applications from page ${pageIndex + 1} of ${pdf.numPages}.`);
        let page = await pdf.getPage(pageIndex + 1);

        // Construct cells (ie. rectangles) based on the horizontal and vertical line segments
        // in the PDF page.

        let cells = await parseCells(page);

        // Construct elements based on the text in the PDF page.

        let elements = await parseElements(page);

        // The co-ordinate system used in a PDF is typically "upside done" so invert the
        // co-ordinates (and so this makes the subsequent logic easier to understand).

        for (let cell of cells)
            cell.y = -(cell.y + cell.height);

        for (let element of elements)
            element.y = -(element.y + element.height);

        // Sort the cells by approximate Y co-ordinate and then by X co-ordinate.

        let cellComparer = (a, b) => (Math.abs(a.y - b.y) < 2) ? ((a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0)) : ((a.y > b.y) ? 1 : -1);
        cells.sort(cellComparer);

        // Sort the text elements by approximate Y co-ordinate and then by X co-ordinate.

        let elementComparer = (a, b) => (Math.abs(a.y - b.y) < 1) ? ((a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0)) : ((a.y > b.y) ? 1 : -1);
        elements.sort(elementComparer);

        // Allocate each element to an "owning" cell.

        for (let element of elements) {
            let ownerCell = cells.find(cell => getPercentageOfElementInCell(element, cell) > 50);  // at least 50% of the element must be within the cell deemed to be the owner
            if (ownerCell !== undefined)
                ownerCell.elements.push(element);
        }

        // Group the cells into rows.

        let rows: Cell[][] = [];

        for (let cell of cells) {
            let row = rows.find(row => Math.abs(row[0].y - cell.y) < 2);  // approximate Y co-ordinate match
            if (row === undefined)
                rows.push([ cell ]);  // start a new row
            else
                row.push(cell);  // add to an existing row
        }

        // Check that there is at least one row (even if it is just the heading row).

        if (rows.length === 0) {
            let elementSummary = elements.map(element => `[${element.text}]`).join("");
            console.log(`No development applications can be parsed from the current page because no rows were found (based on the grid).  Elements: ${elementSummary}`);
            continue;
        }

        // Ensure the rows are sorted by Y co-ordinate and that the cells in each row are sorted
        // by X co-ordinate (this is really just a safety precaution because the earlier sorting
        // of cells in the parseCells function should have already ensured this).

        let rowComparer = (a, b) => (a[0].y > b[0].y) ? 1 : ((a[0].y < b[0].y) ? -1 : 0);
        rows.sort(rowComparer);

        let rowCellComparer = (a, b) => (a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0);
        for (let row of rows)
            row.sort(rowCellComparer);

        // Find the heading cells.

        let applicationNumberCell = cells.find(cell => cell.elements.some(element => element.text.trim() === "APPLICATION"));
        let receivedDateCell = cells.find(cell => cell.elements.some(element => element.text.trim() === "RECEIPT"));
        let houseNumberCell = cells.find(cell => cell.elements.map(element => element.text.trim()).join(" ") === "NO.");
        let lotCell = cells.find(cell => cell.elements.some(element => element.text.trim() === "LOT"));
        let sectionCell = cells.find(cell => cell.elements.some(element => element.text.trim() === "SECTION /"));
        let addressCell = cells.find(cell => cell.elements.some(element => element.text.trim() === "PROPERTY ADDRESS"));
        let descriptionCell = cells.find(cell => cell.elements.some(element => element.text.trim() === "DESCRIPTION"));

        if (applicationNumberCell === undefined) {
            let elementSummary = elements.map(element => `[${element.text}]`).join("");
            console.log(`No development applications can be parsed from the current page because the "ASSESS" column heading was not found.  Elements: ${elementSummary}`);
            continue;
        }

        if (addressCell === undefined) {
            let elementSummary = elements.map(element => `[${element.text}]`).join("");
            console.log(`No development applications can be parsed from the current page because the "PROPERTY ADDRESS" column heading was not found.  Elements: ${elementSummary}`);
            continue;
        }

        // Try to extract a development application from each row (some rows, such as the heading
        // row, will not actually contain a development application).

        for (let row of rows) {
            let rowApplicationNumberCell = row.find(cell => getHorizontalOverlapPercentage(cell, applicationNumberCell) > 90);
            let rowReceivedDateCell = row.find(cell => getHorizontalOverlapPercentage(cell, receivedDateCell) > 90);
            let rowHouseNumberCell = row.find(cell => getHorizontalOverlapPercentage(cell, houseNumberCell) > 90);
            let rowLotCell = row.find(cell => getHorizontalOverlapPercentage(cell, lotCell) > 90);
            let rowSectionCell = row.find(cell => getHorizontalOverlapPercentage(cell, sectionCell) > 90);
            let rowAddressCell = row.find(cell => getHorizontalOverlapPercentage(cell, addressCell) > 90);
            let rowDescriptionCell = row.find(cell => getHorizontalOverlapPercentage(cell, descriptionCell) > 90);

            // Check that there is a valid application number.

            if (rowApplicationNumberCell === undefined)
                continue;
            let applicationNumber = rowApplicationNumberCell.elements.map(element => element.text).join("").trim();
            if (!/[0-9]+\/[0-9]+/.test(applicationNumber))  // an application number must be present, for example, "141/17"
                continue;

            // Construct the address.

            let houseNumber = (rowHouseNumberCell === undefined || rowHouseNumberCell.elements.length === 0 || rowHouseNumberCell.elements[0].text.trim() === "-") ? "" : rowHouseNumberCell.elements[0].text;
            let address = (houseNumber + " " + rowAddressCell.elements.map(element => element.text).join(", ")).replace(/\s\s+/g, " ").trim();
            address = formatAddress(address);
            if (address === "")  // an address must be present
                continue;

            // Construct the description.

            let description = (rowDescriptionCell === undefined) ? "" : rowDescriptionCell.elements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();

            // Construct the received date.

            let receivedDate = moment.invalid();
            if (rowReceivedDateCell !== undefined && rowReceivedDateCell.elements.length > 0)
                receivedDate = moment(rowReceivedDateCell.elements[0].text.trim(), "D/MM/YYYY", true);

            // Construct the legal description.

            let legalElements = [];
            if (rowLotCell !== undefined && rowLotCell.elements.length > 0 && rowLotCell.elements[0].text.trim() !== "-")
                legalElements.push(`Lot ${rowLotCell.elements[0].text}`);
            if (rowSectionCell !== undefined && rowSectionCell.elements.length > 0 && rowLotCell.elements[0].text.trim() !== "-")
                legalElements.push(`Section ${rowSectionCell.elements[0].text}`);
            let hundredElement = rowAddressCell.elements.find(element => element.text.startsWith("HD ") || element.text.toUpperCase().startsWith("HUNDRED "));
            if (hundredElement !== undefined)
                legalElements.push(`Hundred ${hundredElement.text.replace(/^HD /, "").replace(/^HUNDRED /i, "").trim()}`);
            let legalDescription = legalElements.join(", ");

            developmentApplications.push({
                applicationNumber: applicationNumber,
                address: address,
                description: ((description === "") ? "NO DESCRIPTION PROVIDED" : description),
                informationUrl: url,
                commentUrl: CommentUrl,
                scrapeDate: moment().format("YYYY-MM-DD"),
                receivedDate: receivedDate.isValid ? receivedDate.format("YYYY-MM-DD") : "",
                legalDescription: legalDescription
            });        
        }
    }

    return developmentApplications;
}

// Gets a random integer in the specified range: [minimum, maximum).

function getRandom(minimum: number, maximum: number) {
    return Math.floor(Math.random() * (Math.floor(maximum) - Math.ceil(minimum))) + Math.ceil(minimum);
}

// Pauses for the specified number of milliseconds.

function sleep(milliseconds: number) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

// Parses the development applications.

async function main() {
    // Ensure that the database exists.

    let database = await initializeDatabase();

    // Read all street, street suffix, suburb and hundred information.

    readAddressInformation();

    // Read the main page of development applications.

    console.log(`Retrieving page: ${DevelopmentApplicationsUrl}`);

    let body = await request({ url: DevelopmentApplicationsUrl, rejectUnauthorized: false, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);
    let $ = cheerio.load(body);
    
    let pdfUrls: string[] = [];
    for (let element of $("td.u6ListTD a").get()) {
        let pdfUrl = new urlparser.URL(element.attribs.href, DevelopmentApplicationsUrl).href;
        if (pdfUrl.toLowerCase().includes(".pdf"))
            if (!pdfUrls.some(url => url === pdfUrl))  // avoid duplicates
                pdfUrls.push(pdfUrl);
    }

    // Always parse the most recent PDF file and randomly select one other PDF file to parse.

    if (pdfUrls.length === 0) {
        console.log("No PDF URLs were found on the page.");
        return;
    }

    console.log(`Found ${pdfUrls.length} PDF file(s).  Selecting two to parse.`);

    // Select the most recent PDF.  And randomly select one other PDF (avoid processing all PDFs
    // at once because this may use too much memory, resulting in morph.io terminating the current
    // process).

    let selectedPdfUrls: string[] = [];
    selectedPdfUrls.push(pdfUrls.shift());
    if (pdfUrls.length > 0)
        selectedPdfUrls.push(pdfUrls[getRandom(0, pdfUrls.length)]);
    if (getRandom(0, 2) === 0)
        selectedPdfUrls.reverse();

    for (let pdfUrl of selectedPdfUrls) {
        console.log(`Parsing document: ${pdfUrl}`);
        let developmentApplications = await parsePdf(pdfUrl);
        console.log(`Parsed ${developmentApplications.length} development application(s) from document: ${pdfUrl}`);
        
        // Attempt to avoid reaching 512 MB memory usage (this will otherwise result in the
        // current process being terminated by morph.io).

        if (global.gc)
            global.gc();

        console.log(`Inserting development applications into the database.`);
        for (let developmentApplication of developmentApplications)
            await insertRow(database, developmentApplication);
    }
}

main().then(() => console.log("Complete.")).catch(error => console.error(error));

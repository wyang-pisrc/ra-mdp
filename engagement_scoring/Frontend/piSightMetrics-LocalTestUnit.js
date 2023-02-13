

// This method returns the url path in lower case, removing the country substring (unless it is the country homepage).
function getCorePath(url) {
    if (typeof url === "string") {
        if (url.substring(0, 4) === "http") {
            let returnUrl = url.toLowerCase().replace(/https:\/\/[^\/]*\/[a-z][a-z]\-[a-z][a-z][a-z]?(\/.*)|https:\/\/www\.rockwellautomation\.com\.cn(\/.*)|https:\/\/[^\/]*(\/[a-z][a-z]\-[a-z][a-z][a-z]?\.html)/, "$1$2$3");
            if (returnUrl.substring(0, 1) == "/") {
                return returnUrl;
            }
        }
    }
    return null;
}

async function piSightMainTest() {
    resetPiSightStorage();

    TEST_INPUTS = [
        "/products/hardware/allen-bradley.html",
        "/support/product/product-downloads.html",
        "/en-us.html",
        "/company/events.html",
        "/lang-selection.html",
        "/company/events/in-person-events/automation-fair.html",
        "/index.html",
        "/support.html",
    ];
    for (let [i, overridePage] of TEST_INPUTS.entries()) {
        console.log(`${i}-url: ${overridePage}`);
        await piSightMain(overridePage);
    }
}

resetPiSightStorage();
PISIGHT_VERBOSE = 2;
await piSightMainTest();

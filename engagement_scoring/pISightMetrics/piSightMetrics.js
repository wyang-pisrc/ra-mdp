// Configuration: constants
const BIGDECIMAL_DECIMALS = 100; // number of decimals on all instances
const BIGDECIMAL_ROUNDED = true; // numbers are truncated (false) or rounded (true)
const BIGDECIMAL_SHIFT = BigInt("1" + "0".repeat(BIGDECIMAL_DECIMALS)); // derived constant
class BigDecimal {
    constructor(value) {
        if (value instanceof BigDecimal) return value;
        let [ints, decis] = String(value).split(".").concat("");
        this._n = BigInt(ints + decis.padEnd(BIGDECIMAL_DECIMALS, "0").slice(0, BIGDECIMAL_DECIMALS)) + BigInt(BIGDECIMAL_ROUNDED && decis[BIGDECIMAL_DECIMALS] >= "5");
    }
    static fromBigInt(bigint) {
        return Object.assign(Object.create(BigDecimal.prototype), {_n: bigint,});
    }
    add(num) {
        return BigDecimal.fromBigInt(this._n + new BigDecimal(num)._n);
    }
    subtract(num) {
        return BigDecimal.fromBigInt(this._n - new BigDecimal(num)._n);
    }
    static _divRound(dividend, divisor) {
        return BigDecimal.fromBigInt(dividend / divisor + (BIGDECIMAL_ROUNDED ? ((dividend * 2n) / divisor) % 2n : 0n));
    }
    multiply(num) {
        return BigDecimal._divRound(this._n * new BigDecimal(num)._n, BIGDECIMAL_SHIFT);
    }
    divide(num) {
        return BigDecimal._divRound(this._n * BIGDECIMAL_SHIFT, new BigDecimal(num)._n);
    }
    toString() {
        const s = this._n.toString().padStart(BIGDECIMAL_DECIMALS + 1, "0");
        return s.slice(0, -BIGDECIMAL_DECIMALS) + "." + s.slice(-BIGDECIMAL_DECIMALS).replace(/\.?0+$/, "");
    }
    equal(num) {
        return this._n == new BigDecimal(num)._n;
    }
}

async function fetchPageMetrics(corePath) {
    try {
        var params = { method: "GET" };
        var servletPath = PISIGHT_SERVLET_PATH;
        let cacheTime = Date.now() % (3600000 * PISIGHT_CACHE_HOURS); // 24 hour cache -> best pratice -> response header
        let url = `${servletPath}?path=${corePath}&key=autoEScore&Date=${cacheTime}`;
        const response = await fetch(url, params)
            .then((result) => {
                if (result.status != 200) {
                    throw new Error("Bad AEM Server Response");
                }
                return result.text();
            })
            .catch((error) => {
                console.log(error);
            });
        return JSON.parse(response);
    } catch (error) {
        console.warn("fetchPageMetrics error", error);
    }
}

function resetPiSightStorage() {
    window.localStorage.removeItem(PISIGHT_LOCAL_STORAGE_NAME);
    window.localStorage.removeItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME);
}

function showPiSightStorage() {
    console.log("piSight storage with: ", JSON.parse(window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME)));
    console.log("piSight profile storage with: ", JSON.parse(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME)));
}

function storePiSightProfile(profile) {
    window.localStorage.setItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME, JSON.stringify(profile));
}

function getPiSightProfile() {
    return JSON.parse(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME));
}

function showPiSightProfile() {
    console.log("current user profile: ", getPiSightProfile());
}

function showLeadPrediction() {
    userProfiles = JSON.parse(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME));
    console.log(`lead aspect:`);
    for (let metrics of userProfiles["userMetrics"]["lead"]) {
        if (metrics[0] == "lead-Good") {
            console.log(metrics[0], parseFloat(metrics[1]));
        }
    }

    for (let metrics of userProfiles["userMetrics"]["lead"]) {
        if (metrics[0] == "lead-Bad") {
            console.log(metrics[0], parseFloat(metrics[1]));
        }
    }
}

function identifySignal(userProfiles) {
    // TODO: update flag signal
    if (!userProfiles) {
        userProfiles = JSON.parse(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME));
    }

    return true;
}

function getVisitedPages() {
    let currentStorage = getPiSightProfile();
    if (currentStorage && Object.keys(currentStorage).includes("visitedPages")) {
        return currentStorage["visitedPages"];
    } else {
        resetPiSightStorage();
        return [];
    }
}

function getAllPreviousParts(mcvisid) {
    var allVisitedConditionalParts = [];
    var piSight = window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME);

    if (piSight) {
        allVisitedConditionalParts = JSON.parse(piSight);
    }

    if (mcvisid) {
        // TODO: if user visited records upload to cloud. e.g. if user login, then retrieve the data on cloud and override the local one.
    }
    return allVisitedConditionalParts;
}

function calcOneAspectBayesianProbability(updatedParts, aspectProportion) {
    let aspectLabels = Object.keys(aspectProportion);
    var numerators = {};
    var denominator = PISIGHT_DECIMAL_ZERO;
    const isSorted = true;

    for (let label of aspectLabels) {
        let part = new BigDecimal(updatedParts[label]);
        let proportion = new BigDecimal(aspectProportion[label]);
        let products;
        if (PISIGHT_IS_LOG_VERSION) {
            // TODO: exponential support for bigDecimal needed
            products = new BigDecimal(Math.exp(parseFloat(part.add(proportion)))); 
        } else {
            products = part.multiply(proportion);
        }

        numerators = { ...numerators, [label]: products };
        denominator = denominator.add(products);
    }

    var probability = {};

    for (let label of aspectLabels) {
        let labelProb = numerators[label].divide(denominator.equal(PISIGHT_DECIMAL_ZERO) ? "1.0" : denominator).toString();
        probability = { ...probability, [label]: labelProb };
    }

    if (isSorted) {
        probability = Object.entries(probability).sort((a, b) => b[1] - a[1]);
    }
    flag = { [probability[0][0]]: probability[0][1] };
    return [probability, flag];
}

function calcOneAspectParts(aspectKYields, aspectProportion, traffic) {
    let aspectLabels = Object.keys(aspectProportion);
    var parts = {};
    for (let label of aspectLabels) {
        let kYield = new BigDecimal(aspectKYields[label]);
        let proportion = new BigDecimal(aspectProportion[label]);
        let sharedTraffic = new BigDecimal(traffic);
        conditionalProbability = kYield.multiply(sharedTraffic).divide(proportion.equal(PISIGHT_DECIMAL_ZERO) ? "1.0" : proportion);

        parts = { ...parts, [label]: conditionalProbability.toString() };
    }
    return parts;
}

// multiply with previous records
function calcOneCulmulativeParts(previousAspectParts, currentAspectParts) {
    var culmulativeParts = {};
    if (PISIGHT_IS_LOG_VERSION) {
        // log sum version
        for (let label of Object.keys(previousAspectParts)) {
            let previous = parseFloat(previousAspectParts[label]);
            let current = parseFloat(currentAspectParts[label]);
            culmulativeParts = { ...culmulativeParts, [label]: (previous + current).toPrecision(BIGDECIMAL_DECIMALS) };
        }
    } else {
        // no-log multiply version
        for (let label of Object.keys(previousAspectParts)) {
            let previous = new BigDecimal(previousAspectParts[label]);
            let current = new BigDecimal(currentAspectParts[label]);
            culmulativeParts = { ...culmulativeParts, [label]: previous.multiply(current).toString() };
        }
    }

    return culmulativeParts;
}

function getOneAspectMetrics(aspectName, allPageMetrics) {
    let aspectKYields = Object.fromEntries(Object.entries(allPageMetrics["kYieldModified"]).filter(([key]) => key.includes(aspectName)));
    let aspectProportion = Object.fromEntries(Object.entries(allPageMetrics["labelProportion"]).filter(([key]) => key.includes(aspectName)));
    return [aspectKYields, aspectProportion];
}

function isConsistentAspect(allVisitedConditionalParts, allPageMetrics) {
    if (allVisitedConditionalParts == undefined || allVisitedConditionalParts.length == 0) {
        return false;
    }

    if (allVisitedConditionalParts.length > 0) {
        let firstPiSight = allVisitedConditionalParts[0];
        for (let aspect of PISIGHT_ASPECTS) {
            if (!Object.keys(firstPiSight).includes(aspect)) {
                if (SHOW_DEBUG) {
                    console.log("Object.keys(firstPiSight): ", Object.keys(firstPiSight));
                    console.log("page metrics updated, please check ");
                }
                return false;
            }
        }
    }

    return true;
}

// store the current result
function storeVisitedParts(allVisitedConditionalParts) {
    if (allVisitedConditionalParts.length >= PISIGHT_MAX_TRACKING_LENGTH) {
        allVisitedConditionalParts.shift();
    }
    window.localStorage.setItem(PISIGHT_LOCAL_STORAGE_NAME, JSON.stringify(allVisitedConditionalParts));

}

async function piSightMain(overrideCorePath) {
    var corePath = overrideCorePath == undefined ? getCorePath(window.location.href) : overrideCorePath;

    if (corePath == undefined) {
        if (SHOW_DEBUG) {
            console.log("untracking corePath for: ", window.location.href);
        }
        return;
    }

    var visitedPages = getVisitedPages();
    if (visitedPages.includes(corePath)) {
        if (SHOW_DEBUG) {
            showPiSightProfile();
            console.log("already visited this page, no update on metrics");
        }
        return;
    }

    var allPageMetrics = await fetchPageMetrics(corePath);
    if (allPageMetrics == undefined) {
        if (SHOW_DEBUG) {
            console.log("pageMetrics Endpoint is not ready.");
        }
        return;
    }

    var traffic = allPageMetrics["traffic"];
    if (traffic == undefined) {
        if (SHOW_DEBUG) {
            console.log("no valid records for this page URL yet.");
        }
        return;
    }

    var allVisitedConditionalParts = getAllPreviousParts();
    var isNewVisitor;
    var isConsistent = isConsistentAspect(allVisitedConditionalParts, allPageMetrics);
    if (!isConsistent) {
        isNewVisitor = true;
        allVisitedConditionalParts = [];
        resetPiSightStorage();
    } else {
        isNewVisitor = false;
    }

    var userMetrics = {};
    var flags = {};
    var pageConditionalParts = {};

    for (let aspectName of PISIGHT_ASPECTS) {
        let [aspectKYields, aspectProportion] = getOneAspectMetrics(aspectName, allPageMetrics);
        let currentAspectParts = calcOneAspectParts(aspectKYields, aspectProportion, traffic);
        let culmulativeParts = currentAspectParts;
        if (!isNewVisitor) {
            for (let [i, previousAspectParts] of allVisitedConditionalParts.map((part) => part[aspectName]).entries()) {
                culmulativeParts = calcOneCulmulativeParts(previousAspectParts, culmulativeParts);
            }
        }

        let [probability, flag] = calcOneAspectBayesianProbability(culmulativeParts, aspectProportion);

        pageConditionalParts = { ...pageConditionalParts, [aspectName]: currentAspectParts };
        userMetrics = { ...userMetrics, [aspectName]: probability };
        flags = { ...flags, [aspectName]: flag };
    }

    visitedPages.push(corePath); // including this new path to visited pages
    allVisitedConditionalParts.push(pageConditionalParts);
    var userProfiles = {
        userMetrics: userMetrics,
        potentialFlags: flags,
        visitedPages: visitedPages,
    };

    storePiSightProfile(userProfiles); // current user profile
    storeVisitedParts(allVisitedConditionalParts);

    if (SHOW_DEBUG) {
        identifySignal(userProfiles);
        const byteSize = (str) => new Blob([str]).size;
        console.log(
            "piSight local storage byteSize",
            byteSize(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME)) + byteSize(window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME))
        );
    }
    if (SHOW_DEBUG) {
        showLeadPrediction();
    }
}


var PISIGHT_SERVLET_PATH = window.location.origin + "/bin/rockwell-automation/content-score";
// sessionStorage.setItem("piSightHost", "https://qa-aem.rockwellautomation.com")

const PISIGHT_DECIMAL_ZERO = new BigDecimal("0.0");
const PISIGHT_ASPECTS = ["lead", "role", "industry"];
const PISIGHT_LOCAL_STORAGE_NAME = "piSight";
const PISIGHT_PROFILE_LOCAL_STORAGE_NAME = "piSightProfile";
const PISIGHT_IS_LOG_VERSION = false; // NOT FULLY SUPPORT LOG VERSION YET
const PISIGHT_MAX_TRACKING_LENGTH = 15;
const PISIGHT_CACHE_HOURS = 1;

let piSightHost = sessionStorage.getItem("piSightHost");
if (typeof piSightHost !== "undefined" && piSightHost !== null && piSightHost.length > 10) {
    SHOW_DEBUG = true;
    PISIGHT_SERVLET_PATH = piSightHost + "/bin/rockwell-automation/content-score";
}

// resetPiSightStorage();
// await piSightMain();
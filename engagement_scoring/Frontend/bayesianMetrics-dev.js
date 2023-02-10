class BigDecimal {
    // Configuration: constants
    static DECIMALS = 100; // number of decimals on all instances
    static ROUNDED = true; // numbers are truncated (false) or rounded (true)
    static SHIFT = BigInt("1" + "0".repeat(BigDecimal.DECIMALS)); // derived constant
    constructor(value) {
        if (value instanceof BigDecimal) return value;
        let [ints, decis] = String(value).split(".").concat("");
        this._n =
            BigInt(ints + decis.padEnd(BigDecimal.DECIMALS, "0").slice(0, BigDecimal.DECIMALS)) +
            BigInt(BigDecimal.ROUNDED && decis[BigDecimal.DECIMALS] >= "5");
    }
    static fromBigInt(bigint) {
        return Object.assign(Object.create(BigDecimal.prototype), {
            _n: bigint,
        });
    }
    add(num) {
        return BigDecimal.fromBigInt(this._n + new BigDecimal(num)._n);
    }
    subtract(num) {
        return BigDecimal.fromBigInt(this._n - new BigDecimal(num)._n);
    }
    static _divRound(dividend, divisor) {
        return BigDecimal.fromBigInt(dividend / divisor + (BigDecimal.ROUNDED ? ((dividend * 2n) / divisor) % 2n : 0n));
    }
    multiply(num) {
        return BigDecimal._divRound(this._n * new BigDecimal(num)._n, BigDecimal.SHIFT);
    }
    divide(num) {
        return BigDecimal._divRound(this._n * BigDecimal.SHIFT, new BigDecimal(num)._n);
    }
    toString() {
        const s = this._n.toString().padStart(BigDecimal.DECIMALS + 1, "0");
        return s.slice(0, -BigDecimal.DECIMALS) + "." + s.slice(-BigDecimal.DECIMALS).replace(/\.?0+$/, "");
    }
    equal(num) {
        return this._n == new BigDecimal(num)._n;
    }
}

function getCorePath(url) {
    if (typeof url === "string") {
        if (url.substring(0, 4) === "http") {
            let returnUrl = url
                .toLowerCase()
                .replace(
                    /https:\/\/[^\/]*\/[a-z][a-z]\-[a-z][a-z][a-z]?(\/.*)|https:\/\/www\.rockwellautomation\.com\.cn(\/.*)|https:\/\/[^\/]*(\/[a-z][a-z]\-[a-z][a-z][a-z]?\.html)/,
                    "$1$2$3"
                );
            if (returnUrl.substring(0, 1) == "/") {
                return returnUrl;
            }
        }
    }
    return null;
}

async function fetchPageMetrics(corePath) {
    try {
        var params = { method: "GET" };
        var servletPath = SERVLET_PATH;
        let cacheTime = Date.now() % (1000 * 60 * 60 * 1); // 24 hour cache -> best pratice -> response header
        let url = `${servletPath}?path=${corePath}&key=autoEScore&Date=${cacheTime}`;
        console.log(url);
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
    for (metrics of userProfiles["userMetrics"]["lead"]) {
        console.log(metrics[0], parseFloat(metrics[1]));
    }
}

function identifySignal(userProfiles) {
    if (!userProfiles) {
        userProfiles = JSON.parse(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME));
    }

    for (let aspect of Object.keys(userProfiles["potentialFlags"])) {
        console.log(`highest prob in ${aspect} aspect:`);
        console.log(userProfiles["potentialFlags"][aspect]);
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
    var denominator = DECIMAL_ZERO;
    const isSorted = true;

    // no-log multiply version
    for (let label of aspectLabels) {
        let part = new BigDecimal(updatedParts[label]);
        let proportion = new BigDecimal(aspectProportion[label]);
        let products;
        if (IS_LOG_VERSION) {
            products = Math.exp(part.add(proportion)); // it will be really small number as well
        } else {
            products = part.multiply(proportion);
        }

        numerators = { ...numerators, [label]: products };
        denominator = denominator.add(products);
    }

    var probability = {};

    for (let label of aspectLabels) {
        let labelProb = numerators[label].divide(denominator.equal(DECIMAL_ZERO) ? "1.0" : denominator).toString();
        probability = { ...probability, [label]: labelProb };
        if (label.includes("lead") & (VERBOSE == -3)) {
            console.log(label);
            console.log("numerators[label]", numerators[label].toString());
            console.log("labelProb", labelProb.toString());
        }
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
        conditionalProbability = kYield.multiply(sharedTraffic).divide(proportion.equal(DECIMAL_ZERO) ? "1.0" : proportion);
        if (VERBOSE == -1 && label.includes("lead")) {
            console.log(label);
            console.log("kYield", kYield.toString());
            console.log("proportion", proportion.toString());
            console.log("sharedTraffic", sharedTraffic.toString());
        }

        parts = { ...parts, [label]: conditionalProbability.toString() };
    }
    return parts;
}

// multiply with previous records
function calcOneCulmulativeParts(previousAspectParts, currentAspectParts) {
    var culmulativeParts = {};
    if (IS_LOG_VERSION) {
        // log sum version
        for (let label of Object.keys(previousAspectParts)) {
            let previous = parseFloat(previousAspectParts[label]);
            let current = parseFloat(currentAspectParts[label]);
            culmulativeParts = { ...culmulativeParts, [label]: (previous + current).toPrecision(50) };
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
        for (let aspect of ASPECTS) {
            if (!Object.keys(firstPiSight).includes(aspect)) {
                console.log("Object.keys(firstPiSight): ", Object.keys(firstPiSight));
                console.log("page metrics updated, please check ");
                return false;
            }
        }
    }

    return true;
}

// store the current result
function storeVisitedParts(allVisitedConditionalParts) {
    if (allVisitedConditionalParts.length >= MAX_TRACKING_LENGTH) {
        allVisitedConditionalParts.shift();
    }
    window.localStorage.setItem(PISIGHT_LOCAL_STORAGE_NAME, JSON.stringify(allVisitedConditionalParts));

    if (VERBOSE > 2) {
        var i = 0;
        for (k of Object.keys(allVisitedConditionalParts)) {
            i += 1;
            console.log(i, "current allVisitedConditionalParts: ", JSON.stringify(allVisitedConditionalParts[k]));
        }
    }
}

async function piSightMain(overrideCorePath) {
    var corePath = overrideCorePath == undefined ? getCorePath(window.location.href) : overrideCorePath;

    if (corePath == undefined) {
        console.log("untracking corePath for: ", window.location.href);
        return;
    }

    var visitedPages = getVisitedPages();
    if (visitedPages.includes(corePath)) {
        showPiSightProfile();
        console.log("already visited this page, no update on metrics");
        return;
    }

    var allPageMetrics = await fetchPageMetrics(corePath).then((data) => data);
    if (allPageMetrics == undefined) {
        console.log("pageMetrics Endpoint is not ready.");
        return;
    }

    var traffic = allPageMetrics["traffic"];
    if (traffic == undefined) {
        console.log("no valid records for this page URL yet.");
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

    for (let aspectName of ASPECTS) {
        let [aspectKYields, aspectProportion] = getOneAspectMetrics(aspectName, allPageMetrics);
        let currentAspectParts = calcOneAspectParts(aspectKYields, aspectProportion, traffic);
        let culmulativeParts = currentAspectParts;
        if (!isNewVisitor) {
            for (let [i, previousAspectParts] of allVisitedConditionalParts.map((part) => part[aspectName]).entries()) {
                culmulativeParts = calcOneCulmulativeParts(previousAspectParts, culmulativeParts);
            }
        }
        if (VERBOSE == -2 && aspectName.includes("lead")) {
            console.log(`${aspectName}:  ${i},  culmulativeParts: , `, culmulativeParts);
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

    if (VERBOSE > 1) {
        identifySignal(userProfiles);
        const byteSize = (str) => new Blob([str]).size;
        console.log(
            "piSight local storage byteSize",
            byteSize(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME)) + byteSize(window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME))
        );
    }
    if (VERBOSE > 0) {
        showLeadPrediction();
    }
}

async function piSightMainTest() {
    resetPiSightStorage();
    // GOOD CASE IN PYTHON
    // TEST_INPUTS = [
    //     "/products.html",
    //     "/capabilities/industrial-cybersecurity/products-services/vulnerability-assessment.html",
    //     "/products/hardware/allen-bradley/network-security-and-infrastructure/ethernet-networks/stratix-2500-lightly-managed.html",
    //     "/company/events/webinars/technology-partner-genius-webinar-with-spectrum-controls.html",
    // ];

    // BAD CASE IN PYTHON
    TEST_INPUTS = [
        "/capabilities/industrial-automation-control/design-and-configuration-software.html",
        "/en-us.html",
        "/products/software/factorytalk/designsuite/studio-5000/studio-5000-logix-emulate.html",
        "/capabilities/industrial-automation-control/design-and-configuration-software/ccw-software-training-videos.html",
    ];
    for (let [i, overridePage] of TEST_INPUTS.entries()) {
        console.log(i);
        await piSightMain(overridePage);
    }
}

const DECIMAL_ZERO = new BigDecimal("0.0");
const ASPECTS = ["lead", "role", "industry"];
const SERVLET_PATH = window.location.origin + "/bin/rockwell-automation/content-score";
const PISIGHT_LOCAL_STORAGE_NAME = "piSight";
const PISIGHT_PROFILE_LOCAL_STORAGE_NAME = "piSightProfile";
const IS_LOG_VERSION = false; // NOT SUPPORT LOG VERSION YET
const VERBOSE = 1;
const MAX_TRACKING_LENGTH = 20;
// resetPiSightStorage();
// await piSightMain();

await piSightMainTest();

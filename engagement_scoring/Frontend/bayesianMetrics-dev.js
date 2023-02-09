class BigDecimal {
    // Configuration: constants
    static DECIMALS = 80; // number of decimals on all instances
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
        let cacheTime = Date.now() % (1000 * 60 * 60 * 1); // 1 hour cache -> best pratice -> response header
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

function hasPreviousParts() {
    if (typeof Storage !== "undefined") {
        if (window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME)) {
            return true;
        } else {
            return false;
        }
    } else {
        console.log("browser is not supporting local storage for Pisight");
        return false;
    }
}

function loadPreviousParts(aspectName, mcvisid) {
    var piSight = window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME);

    if (piSight) {
        userMetrics = JSON.parse(piSight)[aspectName];
    }

    if (mcvisid) {
        // TODO: if user visited records upload to cloud
        // userMetrics = INIT_METRICS[aspectName];
    }
    return userMetrics;
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
        let kYield = new BigDecimal(aspectProportion[label]);
        let proportion = new BigDecimal(aspectKYields[label]);
        let sharedTraffic = new BigDecimal(traffic);
        conditionalProbability = kYield.multiply(sharedTraffic).divide(proportion.equal(DECIMAL_ZERO) ? "1.0" : proportion);
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

function isConsistentAspect(object) {
    for (let aspect of Object.keys(object)) {
        if (!ASPECTS.includes(aspect)) {
            console.log("Object.keys(object): ", Object.keys(object));
            return false;
        }
    }
    return true;
}
// store the current culmulative result
function storeCulmulativeParts(updatedParts) {
    if (!isConsistentAspect(updatedParts)) {
        console.log("updatedParts: ", updatedParts);
        console.log("Data is not consistent when storing, reset local storage");
        resetPiSightStorage();
    }
    window.localStorage.setItem(PISIGHT_LOCAL_STORAGE_NAME, JSON.stringify(updatedParts));
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

    var userMetrics = {};
    var uploadParts = {};
    var flags = {};
    for (let aspectName of ASPECTS) {
        let [aspectKYields, aspectProportion] = getOneAspectMetrics(aspectName, allPageMetrics);
        let currentAspectParts = calcOneAspectParts(aspectKYields, aspectProportion, traffic);

        if (hasPreviousParts()) {
            previousAspectParts = loadPreviousParts(aspectName, aspectKYields);
            updatedParts = calcOneCulmulativeParts(previousAspectParts, currentAspectParts);
        } else {
            updatedParts = currentAspectParts;
        }

        let [probability, flag] = calcOneAspectBayesianProbability(updatedParts, aspectProportion);

        uploadParts = { ...uploadParts, [aspectName]: updatedParts };
        userMetrics = { ...userMetrics, [aspectName]: probability };
        flags = { ...flags, [aspectName]: flag };
    }

    visitedPages.push(corePath); // including this new path to visited pages
    var userProfiles = {
        userMetrics: userMetrics,
        potentialFlags: flags,
        visitedPages: visitedPages,
    };

    storePiSightProfile(userProfiles);
    storeCulmulativeParts(uploadParts);

    if (VERBOSE) {
        identifySignal(userProfiles);
        const byteSize = (str) => new Blob([str]).size;
        console.log(
            "piSight local storage byteSize",
            byteSize(window.localStorage.getItem(PISIGHT_PROFILE_LOCAL_STORAGE_NAME)) + byteSize(window.localStorage.getItem(PISIGHT_LOCAL_STORAGE_NAME))
        );
    }
}

function piSightMainTest() {
    resetPiSightStorage();
    TEST_INPUTS = [
        "/en-us.html",
        "/capabilities/academy-advanced-manufacturing.html",
        "/support/customer-care.html",
        "/support/product/product-compatibility-migration/migration-modernization.html",
        "/products/hardware/allen-bradley/motion-control.html",
        "/products/software/factorytalk/designsuite/emulate.html",
        "/products/software/factorytalk/operationsuite/mes/plex-quality-management-system.html",
        "/products/software/factorytalk/designsuite.html",
        "/products/software/factorytalk/designsuite/studio-5000/studio-5000-architect.html",
        "/products/software/factorytalk/designsuite/logix-echo.html",
        "/products/software/factorytalk/designsuite/studio-5000/simulation-interface.html",
    ];
    for (let overridePage of TEST_INPUTS) {
        piSightMain(overridePage);
    }
}

const DECIMAL_ZERO = new BigDecimal("0.0");
const ASPECTS = ["lead", "role", "industry"];
const SERVLET_PATH = window.location.origin + "/bin/rockwell-automation/content-score";
const PISIGHT_LOCAL_STORAGE_NAME = "piSight";
const PISIGHT_PROFILE_LOCAL_STORAGE_NAME = "piSightProfile";
const IS_LOG_VERSION = false;
const VERBOSE = true;

// resetPiSightStorage();
// await piSightMain();

piSightMainTest();

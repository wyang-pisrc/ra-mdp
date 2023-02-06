class BigDecimal {
    // Configuration: constants
    static DECIMALS = 50; // number of decimals on all instances
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

async function getData(corePath) {
    try {
        var params = {};
        var servletPath = SERVLET_PATH;
        let cacheTime = Date.now() % (1000 * 60 * 60 * 1); // 1 hour cache
        let url = `${servletPath}?path=${corePath}&key=autoEScore&Date=${cacheTime}`;
        console.log(url);
        const response = await fetch(url, params);
        const resJson = await response.text();
        return JSON.parse(resJson);
    } catch (error) {
        console.warn("getData error", error);
    }

    return null;
}

function resetPiSightCookie() {
    var name = COOKIE_NAME;
    document.cookie = "piSight=" + JSON.stringify(INIT_METRICS);
    var piSight = document.cookie.match(new RegExp(name + "=([^;]+)"));
    console.log("piSight", piSight[1]);
}

function showPiSightCookie() {
    var name = COOKIE_NAME;
    var piSight = document.cookie.match(new RegExp(name + "=([^;]+)"));
    console.log("piSight", piSight[1]);
}

function loadPreviousData(mcvisid) {
    // TODO: update logic to retrieve default value or previous records from APIs
    // load from cookie
    var name = COOKIE_NAME;
    var piSight = document.cookie.match(new RegExp(name + "=([^;]+)"));
    if (piSight) {
        console.log("regex:", piSight[1]);
        // console.log("piSight from Cookie", data);
        userMetrics = JSON.parse(piSight[1]);
    } else if (mcvisid == undefined) {
        userMetrics = INIT_METRICS;
    } else {
        userMetrics = INIT_METRICS;
    }

    return userMetrics;
}

function calcOneAspectBayesianProbability(updatedParts, aspect_proportion) {
    let aspectLabels = Object.keys(aspect_proportion);
    var numerators = {};
    var denominator = DECIMAL_ZERO;
    const isSorted = true;

    // no-log multiply version
    for (let label of aspectLabels) {
        let part = new BigDecimal(updatedParts[label]);
        let proportion = new BigDecimal(aspect_proportion[label]);
        let products;
        if (IS_LOG_VERSION) {
            products = Math.exp(part.add(proportion)); // it will be really small number as well
        } else {
            products = part.multiply(proportion);
        }

        numerators = Object.assign(numerators, {
            [label]: products,
        });
        denominator = denominator.add(products);
    }

    var probability = {};

    for (let label of aspectLabels) {
        let labelProb = numerators[label].divide(denominator.equal(DECIMAL_ZERO) ? "1.0" : denominator).toString();
        probability = Object.assign(probability, {
            [label]: labelProb,
        });
    }

    // sorted
    if (isSorted) {
        probability = Object.entries(probability).sort((a, b) => b[1] - a[1]);
    }
    flag = { [probability[0][0]]: probability[0][1] };
    return [probability, flag];
}

// get the current conditional prob for each labels
function getOneAspectParts(aspect_kYields, aspect_proportion, traffic) {
    let aspectLabels = Object.keys(aspect_proportion);
    var parts = {};
    for (let label of aspectLabels) {
        let kYield = new BigDecimal(aspect_proportion[label]);
        let proportion = new BigDecimal(aspect_kYields[label]);
        let sharedTraffic = new BigDecimal(traffic);
        conditionalProbability = kYield.multiply(sharedTraffic).divide(proportion.equal(DECIMAL_ZERO) ? "1.0" : proportion);
        parts = Object.assign(parts, {
            [label]: conditionalProbability.toString(),
        });
    }
    return parts;
}

// multiply with previous records
function updateCulmulativeParts(previousParts, currentAspectParts, currentAspectName) {
    if (!isConsistentAspect(previousParts) || !previousParts.hasOwnProperty(currentAspectName)) {
        console.log("previousParts: ", previousParts);
        console.log("currentAspectName: ", currentAspectName);
        console.log("currentAspectParts: ", currentAspectParts);
        throw new Error("Data is not consistent when cumulating");
    }

    var previousAspectParts = previousParts[currentAspectName];
    // console.log("previousAspectParts", previousAspectParts);
    var culmulativeParts = {};
    if (IS_LOG_VERSION) {
        // log sum version
        for (let label of Object.keys(previousAspectParts)) {
            let previous = parseFloat(previousAspectParts[label]);
            let current = parseFloat(currentAspectParts[label]);
            culmulativeParts = Object.assign(culmulativeParts, {
                [label]: (previous + current).toPrecision(50),
            });
        }
    } else {
        // no-log multiply version
        for (let label of Object.keys(previousAspectParts)) {
            let previous = new BigDecimal(previousAspectParts[label]);
            let current = new BigDecimal(currentAspectParts[label]);
            culmulativeParts = Object.assign(culmulativeParts, {
                [label]: previous.multiply(current).toString(),
            });
        }
    }

    return culmulativeParts;
}

// store the current culmulative result - tesing on refresh // // upload updatedParts to somewhere
function uploadCulmulativeParts(updatedParts) {
    if (!isConsistentAspect(updatedParts)) {
        console.log("updatedParts: ", updatedParts);
        throw new Error("Data is not consistent when uploading");
    }
    // TODO: upload to cookie?
    document.cookie = "piSight=" + JSON.stringify(updatedParts);
    // console.log("update cookie with", document.cookie)
}

// based on metrics, what kind of user need to be flag
function identifySignal(profiles) {
    return true;
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

async function piSightMain() {
    var corePath = getCorePath(window.location.href);
    if (corePath == undefined) {
        console.log("untracking corePath for: ", window.location.href);
        return;
    }

    var currentMetrics = await getData(corePath).then((data) => data);
    var traffic = currentMetrics["traffic"];
    if (traffic == undefined) {
        console.log("no valid records for this page URL yet.");
        return;
    }
    var aspects = ASPECTS;

    var previousParts = loadPreviousData();
    var result = {};
    var uploadParts = {};
    var flags = {};
    for (let aspectName of aspects) {
        let aspect_kYields = Object.fromEntries(Object.entries(currentMetrics["kYieldModified"]).filter(([key]) => key.includes(aspectName)));
        let aspect_proportion = Object.fromEntries(Object.entries(currentMetrics["labelProportion"]).filter(([key]) => key.includes(aspectName)));
        let currentAspectParts = getOneAspectParts(aspect_kYields, aspect_proportion, traffic);
        updatedParts = updateCulmulativeParts(previousParts, currentAspectParts, aspectName);
        let [probability, flag] = calcOneAspectBayesianProbability(updatedParts, aspect_proportion);

        result = Object.assign(result, {
            [aspectName]: probability,
        });
        uploadParts = Object.assign(uploadParts, {
            [aspectName]: updatedParts,
        });
        flags = Object.assign(flags, {
            [aspectName]: flag,
        });
    }

    var profiles = {
        userMetrics: result,
        flags: flags,
        pageMetrics: currentMetrics["kYieldModified"],
        pageTraffic: currentMetrics["traffic"],
    };

    console.log("profiles : ", profiles);
    console.log("uploadParts: ", uploadParts);
    uploadCulmulativeParts(uploadParts);
}

const DECIMAL_ZERO = new BigDecimal("0.0");
const ASPECTS = ["lead", "role", "industry"];
const SERVLET_PATH = "https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score";
const COOKIE_NAME = "piSight";
const IS_LOG_VERSION = false;
const INIT_METRICS = {
    lead: {
        "lead-Good": "0.00019055395032486849764986964526401956984882146847",
        "lead-Bad": "0.00027838740522546474039834571319626403195745315322",
    },
    role: {
        "role-Csuite": "0.00011615744779056498416210892481674499356931343851",
        "role-Manager": "0.00011200623442667185735535349318170221505174240996",
        "role-Engineer": "0.00044755775063276940941514180745670281190800814769",
        "role-Other": "0.00040814116324538447421497290433989062577104273127",
    },
    industry: {
        "industry-Aerospace": "0.00000091157885784290416497492846986487564034348155",
        "industry-Infrastructure": "0.00000016921523708461095293485821961638219528649226",
        "industry-Automotive_Tire": "0.00019512483167724338414228081546536449695563730748",
        "industry-Cement": "0.00000436138627195497218369754836185589425264761351",
        "industry-Chemical": "0.00000459610418145943264052720439375396816063023979",
        "industry-Entertainment": "0.0000001364639008746862398352694786625589822328429",
        "industry-Fibers_Textiles": "0.00000337884618565723167126024987748037777571159964",
        "industry-Food_Beverage": "0.00004301888011173609608838099929771329911056181043",
        "industry-Glass": "0.00000014192245690967370496780432388387039883992368",
        "industry-HVAC": "0.00000003275133620992470015031868589148233195551014",
        "industry-Household_Personal_Care": "0.00000354806142274184265009364820722144173319437033",
        "industry-Life_Sciences": "0.00000179586493551087108198280304399723948424039453",
        "industry-Marine": "0.00000371727665982645321455040477496759749553668603",
        "industry-Metals": "0.00000961251717761289960418732977717996191827640919",
        "industry-Mining": "0.00000256006278040911381787199124351011568717408726",
        "industry-Oil_Gas": "0.00001866280308362209177594035677367170837121136914",
        "industry-Power_Generation": "0.00000051310426728882032393710950407045800246655754",
        "industry-Print_Publishing": "0.00000061135827591859443733733561680724587943122718",
        "industry-Pulp_Paper": "0.0000248237235737908446106835719587157514220014674",
        "industry-Semiconductor": "0.00000453606006507457106793866340893785958407419841",
        "industry-Whs_EComm_Dist": "0.",
        "industry-Waste_Management": "0.",
        "industry-Water_Wastewater": "0.0000104749690311409165570709045299283530469598853",
        "industry-Other": "0.00028971510705522905333116453825112217874967277765",
    },
};

await piSightMain();

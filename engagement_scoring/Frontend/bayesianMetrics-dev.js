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

async function getData(corePath) {
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
        console.warn("getData error", error);
    }
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
        // console.log("regex:", piSight[1]);
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
    // TODO: upload cookie to piraObj
    document.cookie = "piSight=" + JSON.stringify(updatedParts);
    // console.log("update cookie with", document.cookie)
}

function identifySignal(profiles) {
    // based on metrics, what kind of user need to be flag
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
        potentialFlags: flags,
        pageMetrics: currentMetrics["kYieldModified"],
        pageTraffic: currentMetrics["traffic"],
    };

    console.log("profiles : ", profiles);
    console.log("uploadParts: ", uploadParts);
    uploadCulmulativeParts(uploadParts);
}

// for serving cold start user. Assume they visit the en-us homepage
const INIT_METRICS = {
    lead: { "lead-Good": "0.332185769665836405639680606327601708471775054931640625", "lead-Bad": "0.64247641634327001813886681702570058405399322509765625" },
    role: {
        "role-Csuite": "0.0258060603484974900034831790662792627699673175811767578125",
        "role-Manager": "0.2012114001622929138068940346784074790775775909423828125",
        "role-Engineer": "0.2323283994299165933217210522343521006405353546142578125",
        "role-Other": "0.5087378156955304486785962581052444875240325927734375",
    },
    industry: {
        "industry-Aerospace": "0.003878987921129240130924120677491373498924076557159423828125",
        "industry-Infrastructure": "0.0002424367450705775081827575423432108436827547848224639892578125",
        "industry-Automotive_Tire": "0.070549092815538061440605588359176181256771087646484375",
        "industry-Cement": "0.050184406229609547500647437345833168365061283111572265625",
        "industry-Chemical": "0.0089701595676113694832753964192306739278137683868408203125",
        "industry-Entertainment": "0",
        "industry-Fibers_Textiles": "0.002666804195776352644220441590050540980882942676544189453125",
        "industry-Food_Beverage": "0.2802568773015876590903872056514956057071685791015625",
        "industry-Glass": "0.000484873490141155016365515084686421687365509569644927978515625",
        "industry-HVAC": "0",
        "industry-Household_Personal_Care": "0.030789466623963347713388571946779848076403141021728515625",
        "industry-Life_Sciences": "0.00121218372535288770354411358454171931953169405460357666015625",
        "industry-Marine": "0.00751553909718790345839689592821741825900971889495849609375",
        "industry-Metals": "0.055033141131021097447462153695596498437225818634033203125",
        "industry-Mining": "0.01042478003803483377343042093343683518469333648681640625",
        "industry-Oil_Gas": "0.1069146045761246999195037687968579120934009552001953125",
        "industry-Power_Generation": "0.00096974698028231003273103016937284337473101913928985595703125",
        "industry-Print_Publishing": "0.003151677685917507985846608420388292870484292507171630859375",
        "industry-Pulp_Paper": "0.0654579211690559381597864785362617112696170806884765625",
        "industry-Semiconductor": "0.00969746980282310162835290867633375455625355243682861328125",
        "industry-Whs_EComm_Dist": "0",
        "industry-Waste_Management": "0",
        "industry-Water_Wastewater": "0.0305470298788927674404902745664003305137157440185546875",
        "industry-Other": "0.255528329304388679421577990069636143743991851806640625",
    },
};

const DECIMAL_ZERO = new BigDecimal("0.0");
const ASPECTS = ["lead", "role", "industry"];

const SERVLET_PATH = window.location.origin + "/bin/rockwell-automation/content-score";
// const SERVLET_PATH = "https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score";
// const SERVLET_PATH = "https://qa-aem.rockwellautomation.com/bin/rockwell-automation/content-score";
const COOKIE_NAME = "piSight";
const IS_LOG_VERSION = false;

await piSightMain();

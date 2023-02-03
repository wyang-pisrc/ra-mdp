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


// file is not find 
// not servlet
// edge

getCorePath(window.location.href);

async function getData(corePath) {
    try {
        var params = {};
        var servletPath = SERVLET_PATH;
        let url = `${servletPath}?path=${corePath}&key=autoEScore&akamai=ddd`;
        console.log(url);
        const response = await fetch(url, params);
        const resJson = await response.text();
        return JSON.parse(resJson);
    } catch (error) {
        console.warn("getData error", error);
    }

    return null;
}

function loadPreviousData(mcvisid) {
    if (mcvisid == undefined) {
        // init params for a new user
        cumMetrics = {
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
    } else {
        // retrieve from some places for all the request history of this user
        // assume load from cookie?
        // TODO:
        cumMetrics = {
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
    }

    return cumMetrics;
}

function calcOneCategoryBayesianProbability(updatedParts, category_proportion) {
    let categoryLabels = Object.keys(category_proportion);
    var numerators = {};
    var denominator = DECIMAL_ZERO;

    for (let label of categoryLabels) {
        let part = new BigDecimal(updatedParts[label]);
        let proportion = new BigDecimal(category_proportion[label]);
        let products = part.multiply(proportion);

        numerators = Object.assign(numerators, {
            [label]: products,
        });
        denominator = denominator.add(products);
    }

    var probability = {};
    for (let label of categoryLabels) {
        probability = Object.assign(probability, {
            [label]: numerators[label].divide(denominator.equal(DECIMAL_ZERO) ? "1.0" : denominator).toString(),
        });
    }

    return probability;
}

// get the current conditional prob for each labels
function getOneCategoryParts(category_kYields, category_proportion, traffic) {
    let categoryLabels = Object.keys(category_proportion);
    var parts = {};
    for (let label of categoryLabels) {
        let kYield = new BigDecimal(category_proportion[label]);
        let proportion = new BigDecimal(category_kYields[label]);
        let sharedTraffic = new BigDecimal(traffic);
        conditionalProbability = kYield.multiply(sharedTraffic).divide(proportion.equal(DECIMAL_ZERO) ? "1.0" : proportion);
        parts = Object.assign(parts, {
            [label]: conditionalProbability.toString(),
        });
    }
    return parts;
}

// multiply with previous records
function updateCulmulativeParts(previousParts, currentCategoryParts, currentCategoryName) {
    if (!isConsistentCategory(previousParts) || !previousParts.hasOwnProperty(currentCategoryName)) {
        console.log("previousParts: ", previousParts);
        console.log("currentCategoryName: ", currentCategoryName);
        console.log("currentCategoryParts: ", currentCategoryParts);
        throw new Error("Data is not consistent when cumulating");
    }

    var previousCategoryParts = previousParts[currentCategoryName];
    // console.log("previousCategoryParts", previousCategoryParts);
    var culmulativeParts = {};
    for (let label of Object.keys(previousCategoryParts)) {
        let previous = new BigDecimal(previousCategoryParts[label]);
        let current = new BigDecimal(currentCategoryParts[label]);
        culmulativeParts = Object.assign(culmulativeParts, {
            [label]: previous.multiply(current).toString(),
            // TODO: multiply will lose number, change to add and expoential logarithm
        });
    }
    return culmulativeParts;
}

// store the current culmulative result - tesing on refresh // // upload updatedParts to somewhere
function uploadCulmulativeParts(updatedParts) {
    if (!isConsistentCategory(updatedParts)) {
        console.log("updatedParts: ", updatedParts);
        throw new Error("Data is not consistent when uploading");
    }
    // TODO: upload to cookie?
}

// based on metrics, what kind of user need to be flag
function identifySignal(report) {
    return true;
}

function isConsistentCategory(object) {
    for (category of Object.keys(object)) {
        if (!CATEGORIES.includes(category)) {
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

    // var corePath = "/products/details.1794-oa16.html"
    var currentMetrics = await getData(corePath).then((data) => data);

    var categories = CATEGORIES;
    var traffic = currentMetrics["traffic"];

    var previousParts = loadPreviousData();
    var result = {};
    var uploadParts = {};
    for (let categoryName of categories) {
        let category_kYields = Object.fromEntries(Object.entries(currentMetrics["kYieldModified"]).filter(([key]) => key.includes(categoryName)));
        let category_proportion = Object.fromEntries(Object.entries(currentMetrics["labelProportion"]).filter(([key]) => key.includes(categoryName)));
        let currentCategoryParts = getOneCategoryParts(category_kYields, category_proportion, traffic);
        updatedParts = updateCulmulativeParts(previousParts, currentCategoryParts, categoryName);
        probability = calcOneCategoryBayesianProbability(updatedParts, category_proportion);

        result = Object.assign(result, {
            [categoryName]: probability,
        });
        uploadParts = Object.assign(uploadParts, {
            [categoryName]: updatedParts,
        });
    }

    var report = {
        userMetrics: result,
        pageMetrics: currentMetrics["kYieldModified"],
        // TODO: flag logic here
    };

    console.log("report : ", report);
    console.log("uploadParts: ", uploadParts);
    uploadCulmulativeParts(uploadParts);
}

const DECIMAL_ZERO = new BigDecimal("0.0");
const CATEGORIES = ["lead", "role", "industry"];
const SERVLET_PATH = "https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score";

await piSightMain();

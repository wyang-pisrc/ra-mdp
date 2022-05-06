import streamlit as st
import sqlalchemy
import pandas as pd
import datetime


# === HELPERS ===


def two_weeks_ago():
    date = datetime.date.today() - datetime.timedelta(days=14)
    return date.strftime("%Y-%m-%d")


@st.cache(allow_output_mutation=True)
def get_db_engine():
    server = "sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net"
    database = "Staging"
    username = "pisrc-inkoo"
    with open("./db-pass") as f:
        password = f.read()
    driver = "ODBC Driver 17 for SQL Server"

    return sqlalchemy.create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
    )


@st.cache
def query_db(query, params=None, **kwargs):
    return pd.read_sql(sql=query, con=get_db_engine(), params=params, **kwargs)


def single_result_query(query, params=None, **kwargs):
    df = query_db(query, params=params, **kwargs)
    if df.empty:
        return None
    return df.iat[0, 0]


def tranpose_to_table(df):
    if df is not None and not df.empty:
        styler = df.transpose().style.hide(axis="columns")
        st.write(styler.to_html(), unsafe_allow_html=True)


def get_eloqua_id(email):
    return single_result_query(
        """
        SELECT EloquaContactId
        FROM elq.Contact
        WHERE EmailAddress = ?
        """,
        [email],
    )


# === MODULES ===


def database_info():
    st.markdown(f"**Server**:  \n{single_result_query('SELECT @@VERSION')}")
    st.markdown(f"**Address**:  \n{single_result_query('SELECT @@SERVERNAME')}")
    st.markdown(f"**Database**:  \n{single_result_query('SELECT DB_NAME()')}")


def eloqua_profile(email):
    df = query_db(
        """
        SELECT
            EmailAddress,
            FirstName,
            LastName,
            Company,
            JobFunction,
            JobLevel,
            JobTitle
        FROM elq.Contact
        WHERE EmailAddress = ?
        """,
        [email],
    )
    if df.empty:
        st.write("Eloqua profile not found.")
    else:
        tranpose_to_table(df)


def raw_traffic(eloqua_id):
    df = query_db(
        """
        SELECT
            VisitStartDateTime,
            PageURL,
            UTM_Source,
            UTM_Medium,
            UTM_Campaign,
            UTM_Content,
            UTM_Term
        FROM aem.RawTraffic
        WHERE EloquaContactId = ?
            AND VisitStartDateTime > ?
        """,
        [eloqua_id, two_weeks_ago()],
        parse_dates=["VisitStartDateTime"],
    )
    if df.empty:
        st.write("No Adobe Analytics data found.")
    else:
        st.dataframe(df)


def lead_status(email):
    df = query_db(
        """
        SELECT
            modifiedon,
            ra_leadstagename,
            fullname
        FROM crm.Lead
        WHERE emailaddress1 = ?
        ORDER BY modifiedon
        """,
        [email],
    )
    if df.empty:
        st.write("No CRM lead status found.")
    else:
        st.dataframe(df)


def pathfactory_data(eloqua_id):
    df = query_db(
        """
        SELECT
            SessionStartTime,
            AssetsViewed,
            EngagementScore,
            EngagementTime,
            ExperienceName,
            LastViewedContentSourceURL
        FROM elq.PathFactory
        WHERE EloquaContactId = ?
        ORDER BY SessionStartTime
        """,
        [eloqua_id],
    )
    if df.empty:
        st.write("No Pathfactory data found.")
    else:
        st.dataframe(df)


def traffic_summary(eloqua_id):
    df = query_db(
        """
        SELECT TOP 5
            COUNT(PageURL) as ViewCount,
            PageURL
        FROM aem.RawTraffic
        WHERE EloquaContactId = ?
        AND VisitStartDateTime > ?
        GROUP BY PageURL
        ORDER BY ViewCount DESC
        """,
        [eloqua_id, two_weeks_ago()],
    )
    if not df.empty:
        st.table(df)


def email_lookup():
    email = st.text_input("Please enter an email address:")
    if email:
        eloqua_id = get_eloqua_id(email)
        if eloqua_id is None:
            st.write("Email not found.")
        else:
            st.subheader("Eloqua profile")
            eloqua_profile(email)

            st.subheader("Adobe Analytics page traffic")
            st.write(
                f"Note: Currently limiting data to past two weeks (after {two_weeks_ago()}) for performance."
            )
            st.write("Most frequently viewed pages:")
            traffic_summary(eloqua_id)
            with st.expander("Show raw traffic"):
                raw_traffic(eloqua_id)

            st.subheader("CRM lead status")
            lead_status(email)

            st.subheader("Pathfactory data")
            pathfactory_data(eloqua_id)


def load_examples():
    df = query_db(
        """
        SELECT DISTINCT TOP 20
            e.EmailAddress
        FROM
            aem.RawTraffic as a,
            elq.Contact as e,
            elq.PathFactory as p,
            crm.Lead as l
        WHERE a.VisitStartDateTime > ?
            AND a.EloquaContactId = e.EloquaContactId
            AND e.EmailAddress = l.emailaddress1
            AND e.EloquaContactId = p.EloquaContactId
            AND a.UTM_Source <> ''
        """,
        [two_weeks_ago()],
    )
    st.table(df)


# === LAYOUT ===

st.set_page_config(layout="wide")

st.title("Analytics Dashboard")

st.header("Data source")
database_info()

st.header("Email lookup")
email_lookup()

st.header("Examples")
with st.expander("Show examples"):
    load_examples()

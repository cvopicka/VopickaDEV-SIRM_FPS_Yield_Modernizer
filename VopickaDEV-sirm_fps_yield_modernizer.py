#! /usr/bin/env python3

# region - Dependencies
try:
    errstat = False
    __dependencies__ = []

    trying = "logging"
    __dependencies__.append(trying)
    import logging

    trying = "sys"
    __dependencies__.append(trying)
    import sys

    assert sys.version_info >= (
        3,
        6,
    ), f"Incorrect Python version -- {sys.version_info} -- must be at least 3.6"

    trying = "pathlib"
    __dependencies__.append(trying)
    from pathlib import PurePath

    trying = "datetime.datetime"
    __dependencies__.append(trying)
    from datetime import datetime

    # TODO - Add requirements

    trying = "pyodbc"
    __dependencies__.append(trying)
    import pyodbc

    trying = "pywebio"
    __dependencies__.append(trying)
    from pywebio import (
        input as webinput,
        output as weboutput,
        exceptions as webexceptions,
    )

    trying = "pywebio_battery"
    __dependencies__.append(trying)
    from pywebio_battery import confirm

    trying = "platform"
    __dependencies__.append(trying)
    import platform

    if platform.system() == "windows":
        trying = "asyncio"
        __dependencies__.append(trying)
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    trying = "spf.Config"
    __dependencies__.append(trying)
    from spf.Config import database_dsn

    trying = "toml"
    __dependencies__.append(trying)
    from toml import load as toml_load

except ImportError:
    errstat = True
finally:
    # DOC - Configure the logging system

    # REM - Path tricks for making executable file
    if getattr(sys, "frozen", False):
        runfrom = sys.executable
    else:
        runfrom = __file__

    logging.basicConfig(
        filename=PurePath(
            PurePath(sys.argv[0]).parent,
            "Logs",
            f"{PurePath(sys.argv[0]).stem}-{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.log",
        ),
        format="%(asctime)s-[%(levelname)s]-(%(filename)s)-<%(funcName)s>-#%(lineno)d#-%(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
        filemode="w",
        level=logging.WARNING,
    )

    # REM - Configure a named logger to NOT use the root log
    logger = logging.getLogger(sys.argv[0].replace(".py", ""))
    logger.setLevel(logging.DEBUG)

    # REM - Configure and add console logging to the named logger
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s-[%(levelname)s]-%(message)s"))
    logger.addHandler(console)

    if errstat is True:
        logger.fatal(f"Find missing library! -->{trying}<--")
        raise SystemExit(f"Find missing library! -->{trying}<--")

    # REM - Clean up
    del trying
    del errstat
# endregion - Dependencies

# region Header Block #########################################################
__project__ = runfrom
__purpose__ = "Convert stock FPS YIELD to modern version"
__license__ = "BSD3"
__maintainer__ = "Charles E. Vopicka"
__email__ = "chuck@vopicka.dev"

__status__ = "Prototype"
# __status__ = "Development"
# __status__ = "Production"

__revisionhistory__ = [
    ["Date", "Type", "Author", "Comment"],
    ["2023.08.05", "Created", __maintainer__, "Script Created"],
]
__created__ = __revisionhistory__[1][0]
__author__ = __revisionhistory__[1][2]
__version__ = __revisionhistory__[len(__revisionhistory__) - 1][0]
if __created__.split(".")[0] != __version__.split(".")[0]:
    __copyright__ = f'Copyright {__created__.split(".")[0]} - {__version__.split(".")[0]}, {__maintainer__}'
else:
    __copyright__ = f'Copyright {__created__.split(".")[0]}, {__maintainer__}'

__copyrightstr__ = "This program is licensed under the BSD 3 Clause license\n\n"
__copyrightstr__ += "See the LICENSE file for more information"

__credits__ = []
for n, x in enumerate(__revisionhistory__):
    if x[2] not in __credits__ and n > 0:
        __credits__.append(x[2])

appcredits = "\n".join(
    (
        __purpose__,
        f"\nBy:\t{__author__}",
        f"\t{__email__}",
        "",
        f"License:\t{__license__}",
        "",
        __copyright__,
        "",
        __copyrightstr__,
        f"\nCreated:\t{__created__}",
        f"Version:\t{__version__} ({__status__})",
        f"Rev:\t\t{len(__revisionhistory__) - 1}",
    )
)

logger.info(f"\n\n{appcredits}\n")
weboutput.put_html(f"<h1>{__purpose__}</h1>")
weboutput.put_info(appcredits)

# endregion Header Block ######################################################


# region - Functions here
def IsYieldView() -> bool:
    """Check if the database is non-standard or already converted

    Returns:
        bool: Success or failure
    """

    with pyodbc.connect(
        f"Driver={{{databasedsn['DRIVER']}}};"
        f"Dbq={PurePath(databasedsn['DBQ'])};"
        f"Uid={databasedsn['UID']};"
        f"Pwd=;"
    ) as dbconn:
        with dbconn.cursor() as dbcurs:
            if (
                dbcurs.tables(table="YIELD", tableType="VIEW").fetchone()
                or dbcurs.tables(table="YIELDENS", tableType="VIEW").fetchone()
                or dbcurs.tables(table="YLDSPP", tableType="VIEW").fetchone()
                or dbcurs.tables(table="YLDSRT", tableType="VIEW").fetchone()
                or dbcurs.tables(table="Admin_Meta", tableType="TABLE").fetchone()
            ):
                return True
            else:
                return False


def Conversion() -> bool:
    """The conversion process

    Returns:
        bool: Success or failure
    """

    with pyodbc.connect(
        f"Driver={{{databasedsn['DRIVER']}}};"
        f"Dbq={PurePath(databasedsn['DBQ'])};"
        f"Uid={databasedsn['UID']};"
        f"Pwd=;"
    ) as dbconn:
        with dbconn.cursor() as dbcurs:
            weboutput.put_info("Trying to create Admin_Meta table")
            if CreateAdminMeta(dbconn, dbcurs):
                weboutput.put_success("Created Admin_Meta table")
            else:
                weboutput.put_error(
                    "Something went wrong creating Admin_Meta table.  EXITING"
                )
                return False

            weboutput.put_info("Trying to migrate YIELD data to Admin_Meta")
            if ConvertYIELDtoAdmin_Meta(dbconn, dbcurs):
                weboutput.put_success("Migrated YIELD to Admin_Meta")
            else:
                weboutput.put_error(
                    "Something went wrong migrating data to Admin_Meta.  EXITING"
                )
                return False

            weboutput.put_info("Trying to migrate YIELD data to ADMIN")
            if ConvertYIELDtoADMIN(dbconn, dbcurs):
                weboutput.put_success("Migrated YIELD to ADMIN")
            else:
                weboutput.put_error(
                    "Something went wrong migrating data to ADMIN.  EXITING"
                )
                return False

            weboutput.put_info("Trying to build CRUISE records")
            if ConvertYIELDtoCRUISE(dbconn, dbcurs):
                weboutput.put_success("Built CRUISE records")
            else:
                weboutput.put_error(
                    "Something went wrong building cruise records.  EXITING"
                )
                return False

            weboutput.put_info("Trying to migrate YLDSPP data to PLOTS")
            if ConvertYLDSPPtoPLOTS(dbconn, dbcurs):
                weboutput.put_success("Migrated YLDSPP to PLOTS")
            else:
                weboutput.put_error(
                    "Something went wrong migrating data to PLOTS.  EXITING"
                )
                return False

            weboutput.put_success(
                "Data migration and creation seems to have been programmatically successful.  Verify your data when the whole routine completes."
            )

            weboutput.put_info("Trying to DROP YIELD, YLDENS, YLDSPP, YLDSRT")
            if DropOldTables(dbconn, dbcurs):
                weboutput.put_success("Tables dropped")
            else:
                weboutput.put_error("Something went wrong DROPping tables.  EXITING")
                return False

            weboutput.put_info("Building VIEWs for YIELD, YLDENS, YLDSPP, YLDSRT")
            if CreateViews(dbconn, dbcurs):
                weboutput.put_success("VIEWs created")
            else:
                weboutput.put_error("Something went wrong creating VIEWs")
                return False

    return True


def CreateAdminMeta(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    try:
        curs.execute(sqlstrings["SQL"]["Create"]["Admin_Meta"])
        conn.commit()
    except Exception as ex:
        weboutput.put_warning("Unable to create [Admin_Meta]")
        weboutput.put_error(ex)
        logger.critical("Unable to create [Admin_Meta]")
        logger.exception(ex)
        return False

    return True


def ConvertYIELDtoAdmin_Meta(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    # TODO -
    region = 10
    habgrp = 1
    BHSite = 90

    print(f"{region}{habgrp:0>3}{BHSite:0>3}")
    return False


def ConvertYIELDtoADMIN(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    # TODO -
    return False


def ConvertYIELDtoCRUISE(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    # TODO -
    return False


def ConvertYLDSPPtoPLOTS(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    # TODO -
    return False


def DropOldTables(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    # TODO -
    return False


def CreateViews(conn: pyodbc.Connection, curs: pyodbc.Cursor) -> bool:
    # TODO -
    return False


def GenerateStd_Id(region: int, habgrp: int, bhsite: int) -> int:
    return int(f"{region}{habgrp:0>3}{bhsite:0>3}", base=10)


# endregion - End of functions

# region - Constants
databasedsn = database_dsn()

sqlstrings = toml_load(
    PurePath(
        PurePath(sys.argv[0]).parent,
        "sql.toml",
    )
)

# endregion - Constants

if __name__ == "__main__":
    if IsYieldView():
        msgtxt = "Initial check suggests your database has been converted or is non-standard.  We have terminated for your safety."
        weboutput.put_warning(msgtxt)
        logger.warning(msgtxt)
        raise SystemExit

    backup = confirm(
        "Initial checks suggest this conversion MAY be possible",
        f"Have you backed up the database?\n\n{databasedsn['DBQ']}\n\nThis process can not be reversed.\n\n",
        timeout=30,
    )

    if backup is None:
        weboutput.put_warning("User did not respond in time allowed")
        logger.fatal("No user input provided in time allowed")
        raise SystemExit
    elif not backup:
        weboutput.put_warning("Cannot continue until you have backed up your database!")
        logger.fatal("User has not backed up their database")
        raise SystemExit

    forerealz = confirm(
        "Continue?",
        "I recognize that this option may destroy my database but still want to continue with this hair brained idea.  I accept I have been warned and that I make this decision on my own and accept all consequences of this action.\n\nI want to continue!",
        timeout=30,
    )

    if forerealz is None:
        weboutput.put_warning("User did not respond in time allowed")
        logger.fatal("No user input provided in time allowed")
        raise SystemExit
    elif not forerealz:
        weboutput.put_warning(
            "You have chosen wisely not to continue.  Only once you are sure should you continue."
        )
        logger.info("User terminated session")
        raise SystemExit

    weboutput.put_warning("YOU have opted to continue")
    logger.warning("User opted to continue")

    if Conversion():
        weboutput.put_success(
            "Conversion did not create errors.  Please verify your data."
        )
    else:
        raise SystemExit

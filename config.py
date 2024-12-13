#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os


class DefaultConfig:
    """Bot Configuration"""

    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")

    # GPT-4 Configuration
    GPT4V_NLP_TO_SQL_KEY = os.environ.get("GPT4V_NLP_TO_SQL_KEY", "")
    GPT4V_NLP_TO_SQL_ENDPOINT = os.environ.get("GPT4V_NLP_TO_SQL_ENDPOINT", "")

    GPT4V_SQL_TO_NLP_KEY = os.environ.get("GPT4V_SQL_TO_NLP_KEY", "")
    GPT4V_SQL_TO_NLP_ENDPOINT = os.environ.get(
        "GPT4V_SQL_TO_NLP_ENDPOINT",
        "",
    )

    # SQL Server Configuration
    SQL_SERVER = os.environ.get("SQL_SERVER", "")
    SQL_DB = os.environ.get("SQL_DB", "")
    SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
    SQL_PWD = os.environ.get("SQL_PWD", "")

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 19:02:39 2018

@author: GAS01409
"""

import os
from simple_salesforce import Salesforce
from sfdc_bulk import SalesforceBulkAPI

from dotenv import load_dotenv  # pip install python-dotenv
load_dotenv()

sf = Salesforce(
        username=os.environ.get('USER'),
        password=os.environ.get('PSWD'),
        security_token=os.environ.get('TOKEN'),
        sandbox=False,
        client_id=os.environ.get('CLIENT_ID'),
        )

sf_bulk = SalesforceBulkAPI(username=os.environ.get('USER'),
                            password=os.environ.get('PSWD'),
                            security_token=os.environ.get('TOKEN'),
                            sandbox=False,
                            )
sf_bulk.endpoint = sf.bulk_url

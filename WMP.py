import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
import time
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials


pd.set_option("display.max_columns",None)




def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} took {end - start:.4f} seconds")
        return result
    return wrapper


    
class ERPNext:
    
    DATATABLES = {
        "GL Entry": {
            "doctype": "GL Entry",
            "params":
            {
                "fields" :json.dumps([ 
                    "name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "posting_date", "transaction_date", "account", "party_type",
                    "party", "cost_center", "debit", "credit", 
                    # "account_currency", "debit_in_account_currency", "credit_in_account_currency", 
                    "against",
                    "against_voucher_type", "against_voucher", "voucher_type", "voucher_subtype", "voucher_no", "voucher_detail_no", "project", "remarks",
                    "is_opening", "is_advance", "fiscal_year", "company", "finance_book", "to_rename", "due_date", "is_cancelled", 
                    # "transaction_currency", "debit_in_transaction_currency", "credit_in_transaction_currency", "transaction_exchange_rate",
                    # "_user_tags", "_comments", "_assign",
                    # "_liked_by",
                ]),
                "filters": '[["is_cancelled", "!=", 1]]',
                "limit_page_length": "None"
            }
        },
        "Purchase Invoice": {
            "doctype": "Purchase Invoice",
            "params":
            {
                "fields" :json.dumps([
                    "name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "title", "naming_series", "supplier", "supplier_name",
                    "tax_id", "company", "posting_date", "posting_time", "set_posting_time", "due_date", "is_paid", "is_return", "return_against",
                    "update_outstanding_for_self", "update_billed_amount_in_purchase_order", "update_billed_amount_in_purchase_receipt", "apply_tds",
                    "tax_withholding_category", "amended_from", "bill_no", "bill_date", "cost_center", "project", "currency", "conversion_rate", 
                    "use_transaction_date_exchange_rate", "buying_price_list", "price_list_currency", "plc_conversion_rate", "ignore_pricing_rule", 
                    "scan_barcode", "update_stock", "set_warehouse", "set_from_warehouse", "is_subcontracted", "rejected_warehouse", "supplier_warehouse",
                    "total_qty", "total_net_weight", "base_total", "base_net_total", "total", "net_total", "tax_withholding_net_total", 
                    "base_tax_withholding_net_total", "tax_category", "taxes_and_charges", "shipping_rule", "incoterm", "named_place",
                    "base_taxes_and_charges_added", "base_taxes_and_charges_deducted", "base_total_taxes_and_charges", "taxes_and_charges_added",
                    "taxes_and_charges_deducted", "total_taxes_and_charges", "base_grand_total", "base_rounding_adjustment", "base_rounded_total", 
                    "base_in_words", "grand_total", "rounding_adjustment", "use_company_roundoff_cost_center", "rounded_total", "in_words",
                    "total_advance", "outstanding_amount", "disable_rounded_total", "apply_discount_on", "base_discount_amount",
                    "additional_discount_percentage", "discount_amount", "other_charges_calculation", "mode_of_payment", "base_paid_amount",
                    "clearance_date", "cash_bank_account", "paid_amount", "allocate_advances_automatically", "only_include_allocated_payments", 
                    "write_off_amount", "base_write_off_amount", "write_off_account", "write_off_cost_center", "supplier_address", "address_display", 
                    "contact_person", "contact_display", "contact_mobile", "contact_email", "shipping_address", "shipping_address_display", 
                    "billing_address", "billing_address_display", "payment_terms_template", "ignore_default_payment_terms_template", "tc_name",
                    "terms", "status", "per_received", "credit_to", "party_account_currency", "is_opening", "against_expense_account", 
                    "unrealized_profit_loss_account",
                    #"repost_required",
                    "subscription", "auto_repeat", "from_date", "to_date", "letter_head",
                    "group_same_items", "select_print_heading", "language", "on_hold", "release_date", "hold_comment", "is_internal_supplier", 
                    "represents_company", "supplier_group", "inter_company_invoice_reference", "is_old_subcontracting_flow", "remarks", 
                    # "_user_tags", "_comments", "_assign", "_liked_by",
                ]),
                "filters": '[["status", "NOT IN", ["Cancelled", "Draft"]]]',
                "limit_page_length": "None"
            }
        },
        "Account": {
            "doctype": "Account",
            "params":
            {
                "fields" :json.dumps([
                    "name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "disabled", "account_name", "account_number",
                    "is_group", "company", "root_type", "report_type", "account_currency", "parent_account", "account_type", "tax_rate", "freeze_account",
                    "balance_must_be", "lft", "rgt", "old_parent", "include_in_gross",
                    # "_user_tags", "_comments", "_assign", "_liked_by",
                ]),
                "filters": '',
                "limit_page_length": "None"
            }
        },
        "Cost Center": {
            "doctype": "Cost Center",
            "params":
            {
                "fields" :json.dumps([
                    "name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "cost_center_name", "cost_center_number",
                    "parent_cost_center", "company", "is_group", "disabled", "lft", "rgt", "old_parent",
                    # "_user_tags", "_comments", "_assign", "_liked_by",
                ]),
                "filters": '',
                "limit_page_length": "None"
            }
        },
        "Purchase Invoice Item": {
            "doctype": "items",
            "parent_doctype": "Purchase Invoice",
            "params":
            {
                "filters": '',
                "limit_page_length": "None"
            }
        },
        "Payment Entry": {
            "doctype": "Payment Entry",
            "params":
            {
                "fields" :json.dumps([
                    "name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "naming_series", "payment_type", "payment_order_status",
                    "posting_date", "company", "mode_of_payment", "party_type", "party", "party_name", "book_advance_payments_in_separate_party_account",
                    "bank_account", "party_bank_account", "contact_person", "contact_email", "party_balance", "paid_from", "paid_from_account_type",
                    "paid_from_account_currency", "paid_from_account_balance", "paid_to", "paid_to_account_type", "paid_to_account_currency", 
                    "paid_to_account_balance", "paid_amount", "paid_amount_after_tax", "source_exchange_rate", "base_paid_amount",
                    "base_paid_amount_after_tax", "received_amount", "received_amount_after_tax", "target_exchange_rate", "base_received_amount",
                    "base_received_amount_after_tax", "total_allocated_amount", "base_total_allocated_amount", "unallocated_amount", "difference_amount",
                    "purchase_taxes_and_charges_template", "sales_taxes_and_charges_template", "apply_tax_withholding_amount", "tax_withholding_category",
                    "base_total_taxes_and_charges", "total_taxes_and_charges", "reference_no", "reference_date", "clearance_date", "project", 
                    "cost_center", "status", "custom_remarks", "remarks", "base_in_words", "letter_head", "print_heading", "bank", "bank_account_no",
                    "payment_order", "in_words", "auto_repeat", "amended_from", "title", "_user_tags", "_comments", "_assign", "_liked_by",
                ]),
                "filters": '[["status", "NOT IN", ["Cancelled", "Draft"]]]',
                "limit_page_length": "None"
            }
        },
        "Sales Invoice": {
            "doctype": "Sales Invoice",
            "params":
            {
                "fields" :json.dumps([
                    "name", "creation", "modified", "modified_by", "owner", "docstatus", "idx", "title", "naming_series", "customer", "customer_name", "tax_id",
                    "company", "company_tax_id", "posting_date", "posting_time", "set_posting_time", "due_date", "is_pos", "pos_profile", "is_consolidated",
                    "is_return", "return_against", "update_outstanding_for_self", "update_billed_amount_in_sales_order", "update_billed_amount_in_delivery_note",
                    "is_debit_note", "amended_from", "cost_center", "project", "currency", "conversion_rate", "selling_price_list", "price_list_currency", 
                    "plc_conversion_rate", "ignore_pricing_rule", "scan_barcode", "update_stock", "set_warehouse", "set_target_warehouse", "total_qty", 
                    "total_net_weight", "base_total", "base_net_total", "total", "net_total", "tax_category", "taxes_and_charges", "shipping_rule", "incoterm",
                    "named_place", "base_total_taxes_and_charges", "total_taxes_and_charges", "base_grand_total", "base_rounding_adjustment", 
                    "base_rounded_total", "base_in_words", "grand_total", "rounding_adjustment", "use_company_roundoff_cost_center", "rounded_total", "in_words",
                    "total_advance", "outstanding_amount", "disable_rounded_total", "apply_discount_on", "base_discount_amount", "is_cash_or_non_trade_discount",
                    "additional_discount_account", "additional_discount_percentage", "discount_amount", "other_charges_calculation", "total_billing_hours",
                    "total_billing_amount", "cash_bank_account", "base_paid_amount", "paid_amount", "base_change_amount", "change_amount",
                    "account_for_change_amount", "allocate_advances_automatically", "only_include_allocated_payments", "write_off_amount",
                    "base_write_off_amount", "write_off_outstanding_amount_automatically", "write_off_account", "write_off_cost_center", "redeem_loyalty_points",
                    "loyalty_points", "loyalty_amount", "loyalty_program", "loyalty_redemption_account", "loyalty_redemption_cost_center", "customer_address",
                    "address_display", "contact_person", "contact_display", "contact_mobile", "contact_email", "territory", "shipping_address_name",
                    "shipping_address", "dispatch_address_name", "dispatch_address", "company_address", "company_address_display", 
                    "ignore_default_payment_terms_template", "payment_terms_template", "tc_name", "terms", "po_no", "po_date", "debit_to", 
                    "party_account_currency", "is_opening", "unrealized_profit_loss_account", "against_income_account", "sales_partner",
                    "amount_eligible_for_commission", "commission_rate", "total_commission", "letter_head", "group_same_items", "select_print_heading", "language",
                    "subscription", "from_date", "auto_repeat", "to_date", "status", "inter_company_invoice_reference", "campaign", "represents_company", "source",
                    "customer_group", "is_internal_customer", "is_discounted", "remarks", 
                    # "repost_required",
                    "_user_tags", "_comments", "_assign", "_liked_by",
                    # "_see",
                ]),
                "filters": '',
                "limit_page_length": "None"
            }
        },
        "Sales Invoice Item": {
            "doctype": "items",
            "parent_doctype": "Sales Invoice",
            "params":
            {
                "limit_page_length": "None"
            }
        },
        "Payment Entry Reference": {
            "doctype": "references",
            "parent_doctype": "Payment Entry",
            "params":
            {
                "limit_page_length": "None"
            }
        },
    }
    
    def __init__(self, base_url, auth):
        
        # ERPNext URL and endpoint
        self.base_url = base_url
        self.data = dict()
        self.headers = dict()
        self.add_to_headers("Authorization", auth)
        

    def add_to_headers(self, key, value):
        self.headers[key] = value
        return self.headers
        
    def update_headers(self, headers):
        self.headers.update(headers)
        return self.headers

    @classmethod
    def __fields_func(fields): 
        return f"""["{'","'.join([i for i in fields])}"]"""

    def data_endpoint(self, base_url, doctype): 
        return f"{base_url}/api/resource/{doctype}"


    def child_data_endpoint(self, base_url, doctype, parent_id):
        return f"{base_url}/api/resource/{doctype}/{parent_id}"


    def get_available_datatables(self):
        return list(ERPNext.DATATABLES.keys())
    
    def __read_datatable(self, doctype, headers="default"):

        headers = self.headers if headers=='default' else headers
        
        datatable = ERPNext.DATATABLES.get(doctype)
        
        if datatable is None:
            print(f"Error: ({doctype}) doesn't exist.")
            return
            
        if datatable.get('parent_doctype') == None:
            data_response = requests.get(self.data_endpoint(self.base_url, datatable['doctype']), params=datatable['params'], headers=headers)
            
            # Check response
            if data_response.status_code == 200:
                df = pd.DataFrame(data_response.json()['data'])
                self.data[doctype] = df
                return {datatable['doctype']: df}
            else:
                print(f"Error: ({datatable['doctype']})", data_response.status_code, data_response.text)
        
        else:
            parent_datatable = ERPNext.DATATABLES.get(datatable['parent_doctype']) #[i for i in ERPNext.DATATABLES if i['doctype']==datatable['parent_doctype']][0]
            if parent_datatable is None:
                print(f"Error: ({datatable['doctype']}) Failed to get parent data")
                return
                
            parent_params = parent_datatable['params'].copy()
            parent_params.pop('fields',None)
            
            parent_response = requests.get(self.data_endpoint(self.base_url, parent_datatable['doctype']), params=parent_params, headers=headers)
            child_responses = []
            for parent_name in parent_response.json()['data']:
                parent_id = parent_name['name']
                response = requests.get(self.child_data_endpoint(self.base_url, datatable['parent_doctype'], parent_id), params=datatable['params'], headers=headers)
                if response.status_code == 200:
                    child_responses.append(pd.DataFrame(response.json()['data'][datatable['doctype']]))
                else:
                    print(f"Error: ({doctype})", data_response.status_code, data_response.text)
             
            if len(child_responses)>0:
                df = pd.concat(child_responses)
                self.data[doctype] = df
                return {doctype: df}
        
    @timer
    def read_datatables(self, doctypes='all'):
        if doctypes == 'all':
            doctypes = self.get_available_datatables()

        doctypes = doctypes if isinstance(doctypes, list) else [doctypes]
        
        data = dict()    
        for doctype in doctypes:
            _ = self.__read_datatable(doctype)
            if _ is not None:
                data.update(_)
        if len(data.keys())>0:
            self.data.update(data)
            return data if len(data.keys())>1 else data[list(data.keys())[0]]



    def prep_data(self, df, doctype):
        # if doctypes == 'all':
        #     doctypes = self.get_available_datatables()
        
        # if isinstance(doctypes, list):
        #     prep_data = dict()    
        #     for doctype in doctypes:

        if doctype == 'Purchase Invoice':
            def quality_check():
                pass
                # Accounts codes distribution
                # dfs['Account'].assign(acc_type = lambda x: x['account_number'].str[0])\
                # .groupby('acc_type')['root_type'].value_counts()

                # PINVI Purchase Orders !!!!
                # dfs['Purchase Invoice Item'][lambda x: ~x['po_detail'].isna()][pinvi_columns]


                # PINV
                    # Invoince with total of zero
                    # dfs['Purchase Invoice'][lambda x : x['grand_total']==0][pinv_coulms]

                    # Returned Invoices with no return_against
                    # dfs['Purchase Invoice'][lambda x: x['status'].isin(['Return'])][lambda x: x['return_against'].str.len() < 2]

                # Accounting Error Detection
                    # 1. A return Purchase Invoice with no Return Against shoult always equal 0
                    
                    
                    # 1. Shouldn't purchases relate to a cost center?
                    
                    #             cost_center	
                    # count	    275	
                    # unique	    3
                    # top	        Main - روائع
                    # freq	    266
                    
                    # 2. Some invoices doesn't have vat (total_taxes_and_charges)
                    # 3. Some invoice have grand total of zero (ACC-PINV-2025-00096-1)

                # PAY
                    # Difference between amoutn columns
                    # dfs['Payment Entry'][
                    #     lambda x:
                    #     (
                    #         x['base_paid_amount']- \
                    #         x['base_paid_amount_after_tax']-\
                    #         x['received_amount']-\
                    #         x['received_amount_after_tax']+ \
                    #         x['base_received_amount']+\
                    #         x['base_received_amount_after_tax']
                    #     ) != 0
                    # ][pay_columns]

            pinv_columns = \
            [
                'name', 'title', 'supplier', 'supplier_name', 'posting_date', 'posting_time', 'due_date','return_against', 'bill_no', 'bill_date', 'cost_center', 'total_qty', 'tax_category'
                , 'total_taxes_and_charges', 'grand_total', 'discount_amount', 'supplier_address', 'status', 'credit_to', 'against_expense_account'#rename to debit_to
                ,'supplier_group'
                , 'remarks'
            ]
            
            df = df[pinv_columns].copy()
            
            df = df.rename({
                'credit_to':'credit_account',
                'against_expense_account':'debit_account'
            }, axis=1)
            
            return df

            
        elif doctype == 'Purchase Invoice Item':
            pinvi_columns = [
                'name','item_code','item_name','description','item_group','qty','uom','amount','expense_account',
                'is_fixed_asset','asset_category','cost_center','parent','purchase_order','po_detail'
            ]
            
            # Fixing description column
            df["description"] = df["description"].apply(
                lambda x: BeautifulSoup(x, "html.parser").get_text(" ", strip=True)
            )

            # The naming for some item equals the code, for those specific entries replace the name with the description
            df.loc[lambda x: x['item_code']==x['item_name'], 'item_name'] = df.loc[lambda x: x['item_code']==x['item_name'], 'description']
            return df[pinvi_columns]

        elif doctype == 'Payment Entry':
            # pay_columns = \÷
            pay_columns = [
                'name', 'creation', 'modified', 'modified_by', 'owner', 'payment_type', 'posting_date', 'mode_of_payment', 'party_type', 'party', 'party_name',
                'paid_from', 'paid_from_account_type', 'paid_from_account_balance', 'paid_to', 'paid_to_account_type', 'paid_to_account_balance', 'paid_amount',
                'received_amount', 'reference_no', 'reference_date', 'cost_center', 'status', 'custom_remarks' , 'remarks', 'remarks',
                # 'base_paid_amount',
                # 'base_paid_amount_after_tax',
                # 'received_amount',
                # 'received_amount_after_tax',
                # 'base_received_amount',
                # 'base_received_amount_after_tax',
                # 'total_allocated_amount',
                # 'base_total_allocated_amount',
            ]
            return df[pay_columns]

        elif doctype == 'GL Entry':
            gl_columns = [
                'posting_date', 'debit', 'credit', 'account', 'against', 'voucher_no', 'against_voucher', 'voucher_type', 'against_voucher_type',
                'party_type', 'party', 'cost_center', 'remarks',
            ]
            return df[gl_columns]

        elif doctype == 'Payment Entry Reference':
            payr_columns = ['name', 'reference_doctype', 'reference_name', 'allocated_amount', 'parent']
            
            return df[payr_columns].rename(
                {
                    'parent': 'payment_no',
                    'reference_name': 'invoice_no',
                    'reference_doctype': 'invoice_type',
                    'allocated_amount': 'amount'
                }, axis = 1
            )

        elif doctype == 'Sales Invoice':
            pinv_columns = \
            [
                'name', 'title', 'customer', 'customer_name', 'posting_date', 'posting_time', 'due_date','return_against', 'cost_center', 'total_qty', 'tax_category'
                , 'total_taxes_and_charges', 'grand_total', 'discount_amount', 'customer_address', 'status',
                ,'customer_group'
                , 'remarks'
            ]
            
            df = df[pinv_columns].copy()
            
            df = df.rename({
                'credit_to':'credit_account',
                'against_expense_account':'debit_account'
            }, axis=1)
            
            return df

            
        elif doctype == 'Sales Invoice Item':
            pinvi_columns = [
                'name','item_code','item_name','description','item_group','qty','uom','amount','expense_account', 'income_account',
                'cost_center','parent'
            ]
            
            # Fixing description column
            df["description"] = df["description"].apply(
                lambda x: BeautifulSoup(x, "html.parser").get_text(" ", strip=True)
            )

            # The naming for some item equals the code, for those specific entries replace the name with the description
            df.loc[lambda x: x['item_code']==x['item_name'], 'item_name'] = df.loc[lambda x: x['item_code']==x['item_name'], 'description']
            return df[pinvi_columns]

        else:
            return df


    def prep_dfs(self, dfs):
        dfs_prep = dict()
        
        for doctype, df in dfs.items():
            dfs_prep[doctype] = en.prep_data(df, doctype)

        return dfs_prep

        
    
    def export_dataframes_to_gsheet(self, dfs, spreadsheet_id, mode='overwrite'):
        """
        Export a dictionary of DataFrames to an existing Google Sheet.
    
        Parameters:
        dfs (dict): {sheet_name: dataframe}
        spreadsheet_id (str): The ID of the Google Sheet
        mode (str): 'overwrite' to replace existing sheet content, 'append' to add after existing rows
        """
        # Authenticate
    
        token = "shopal-299823-30fe44932863.json"
        creds = Credentials.from_service_account_file(
            token,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
    
        # Open spreadsheet
        sh = client.open_by_key(spreadsheet_id)
    
        for sheet_name, df in dfs.items():
            try:
                # Try to open existing sheet
                worksheet = sh.worksheet(sheet_name)
                print(f"Sheet '{sheet_name}' exists.")
    
                if mode == 'overwrite':
                    worksheet.clear()
                    set_with_dataframe(worksheet, df)
                    print(f"Overwritten '{sheet_name}'.")
                
                elif mode == 'append':
                    existing_df = get_as_dataframe(worksheet, evaluate_formulas=True).dropna(how="all")
                    start_row = len(existing_df) + 1
                    set_with_dataframe(worksheet, df, row=start_row, include_column_header=False)
                    print(f"Appended {len(df)} rows to '{sheet_name}'.")
                
                else:
                    raise ValueError("Mode must be 'overwrite' or 'append'.")
    
            except gspread.exceptions.WorksheetNotFound:
                # Create new sheet if it doesn't exist
                print(f"Creating new sheet '{sheet_name}'.")
                sh.add_worksheet(title=sheet_name, rows=str(len(df) + 10), cols=str(len(df.columns) + 10))
                worksheet = sh.worksheet(sheet_name)
                set_with_dataframe(worksheet, df)
                print(f"Created and wrote data to '{sheet_name}'.")

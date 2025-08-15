import requests
import pandas as pd
import json
from bs4 import BeautifulSoup

class ERPNext:
    
    DATATABLES = [
        {
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
        {
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
        {
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
        {
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
        {
            "doctype": "items",
            "parent_doctype": "Purchase Invoice",
            "params":
            {
                "filters": '',
                "limit_page_length": "None"
            }
        },
        {
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
                "filters": '',
                "limit_page_length": "None"
            }
        },
    ]
    
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

    def get_available_datatables(self):
        return [i['doctype'] for i in ERPNext.DATATABLES]
    
    def __read_datatable(self, doctype, headers="default"):

        headers = self.headers if headers=='default' else headers
        
        datatable = [i for i in ERPNext.DATATABLES if i['doctype']==doctype]
        
        if len(datatable)>0:
            datatable = datatable[0]
        else:
            print(f"Error: ({doctype}) doesn't exist.")
            return
            
        if datatable.get('parent_doctype') == None:
            data_response = requests.get(data_endpoint(self.base_url, datatable['doctype']), params=datatable['params'], headers=headers)
            
            # Check response
            if data_response.status_code == 200:
                df = pd.DataFrame(data_response.json()['data'])
                self.data[datatable['doctype']] = df
                return df
            else:
                print(f"Error: ({datatable['doctype']})", data_response.status_code, data_response.text)
        
        else:
            parent_datatable = [i for i in ERPNext.DATATABLES if i['doctype']==datatable['parent_doctype']][0]
            parent_params = parent_datatable['params'].copy()
            parent_params.pop('fields',None)
            
            parent_response = requests.get(data_endpoint(self.base_url, parent_datatable['doctype']), params=parent_params, headers=headers)
            child_responses = []
            for parent_name in parent_response.json()['data']:
                parent_id = parent_name['name']
                response = requests.get(child_data_endpoint(self.base_url, datatable['parent_doctype'], parent_id), params=datatable['params'], headers=headers)
                if response.status_code == 200:
                    child_responses.append(pd.DataFrame(response.json()['data'][datatable['doctype']]))
                else:
                    print(f"Error: ({datatable['doctype']}/{parent_name})", data_response.status_code, data_response.text)
             
            if len(child_responses)>0:
                df = pd.concat(child_responses)
                self.data[f"{datatable['doctype']}"] = df
                return df
        

    def read_datatables(self, doctypes='all'):
        if doctypes == 'all':
            doctypes = self.get_available_datatables()
        
        if isinstance(doctypes, list):
            data = dict()    
            for doctype in doctypes:
                _ = self.__read_datatable(doctype)
                if _ is not None:
                    data[doctype] = _
            self.data.update(data)
            return data
        else:
            return self.__read_datatable(doctypes)


    def prep_data(self, df, doctype):
        # if doctypes == 'all':
        #     doctypes = self.get_available_datatables()
        
        # if isinstance(doctypes, list):
        #     prep_data = dict()    
        #     for doctype in doctypes:

        if doctype == 'Purchase Invoice':
            def quality_check():
                pass

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

        elif doctype == 'Payment Entry':
            pinv_columns = \
            [
                'name', 'item_code', 'item_name', 'description', 'item_group', 'qty', 'uom', 'amount', 'expense_account', 'is_fixed_asset',
                'asset_category', 'cost_center', 'parent', 'purchase_order', 'po_detail'
            ]
            return df[pinv_columns]

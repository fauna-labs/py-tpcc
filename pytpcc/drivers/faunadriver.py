from __future__ import with_statement

import os
import logging
from pprint import pprint,pformat

from fauna import fql
from fauna.client import Client, QueryOptions
from fauna.encoding import QuerySuccess, QueryStats
from fauna.errors import FaunaException, AbortError

import requests
from datetime import datetime, timedelta
from sys import getsizeof
import random
import json

import constants
from .abstractdriver import AbstractDriver


TABLES = {
    constants.TABLENAME_ITEM: {
        "columns": [
            "I_ID", # INTEGER
            "I_IM_ID", # INTEGER
            "I_NAME", # VARCHAR
            "I_PRICE", # FLOAT
            "I_DATA", # VARCHAR
        ],
        "schema": {
            "history_days": 0,            
            "indexes": {
              "byItemId": {
                "terms": [{"field": ".I_ID"}]
              }
            },
            "constraints": [
              {
                "unique": [{"field": ".I_ID"}]
              }
            ]
        }
    },
    constants.TABLENAME_WAREHOUSE: {
        "columns": [
            "W_ID", # SMALLINT
            "W_NAME", # VARCHAR
            "W_STREET_1", # VARCHAR
            "W_STREET_2", # VARCHAR
            "W_CITY", # VARCHAR
            "W_STATE", # VARCHAR
            "W_ZIP", # VARCHAR
            "W_TAX", # FLOAT
            "W_YTD", # FLOAT
        ],
        "schema": {
            "history_days": 0,            
            "computed_fields": {
              "YTD_TOTAL": {
                "body": "(w) => {WAREHOUSE_YTD.byWarehouse(w).where(.W_YTD > 0).map((x) => x.W_YTD).aggregate(0.00, (val, x) => val + x)}"
              }
            },
            "indexes": {
              "byWarehouseId": {
                "terms": [{"field": ".W_ID"}]
              }
            },
            "constraints": [
              {
                "unique": [{"field": ".W_ID"}]
              }
            ]            
        },
        "children": [
            {
                "name": "WAREHOUSE_YTD",
                "schema": {
                    "constraints": [],
                    "history_days": 0,
                    "indexes": {
                      "byWarehouse": {
                        "terms": [
                          {"field": ".warehouse"}
                        ],
                        "values": [
                          {"field": ".shard", "order": "asc"},
                          {"field": ".W_YTD", "order": "asc"}
                        ]
                      },
                      "byWarehouseAndShard": {
                        "terms": [
                          {"field": ".warehouse"},
                          {"field": ".shard"}
                        ],
                        "values": [
                          {"field": ".W_YTD", "order": "asc"}
                        ]
                      }
                    }                        
                },
                "fql": {
                    "query": 
                    """
                    list1000.forEach(x => {
                      WAREHOUSE_YTD.create({
                        shard: x,
                        warehouse: doc,
                        W_ID: doc!.W_ID,
                        W_YTD: if (x == 1) { doc!.W_YTD } else { 0 }
                      })
                    })
                    """,
                    "arguments": [{
                        "name": "list1000",
                        "value": list(range(1, 1001))
                    }]       
                }
            }
        ]
    },
    constants.TABLENAME_DISTRICT: {
        "columns": [
            "D_ID", # TINYINT
            "D_W_ID", # SMALLINT
            "D_NAME", # VARCHAR
            "D_STREET_1", # VARCHAR
            "D_STREET_2", # VARCHAR
            "D_CITY", # VARCHAR
            "D_STATE", # VARCHAR
            "D_ZIP", # VARCHAR
            "D_TAX", # FLOAT
            "D_YTD", # FLOAT
            "D_NEXT_O_ID", # INT
        ],
        "schema": {
            "history_days": 0,            
            "computed_fields": {
              "YTD_TOTAL": {
                "body": "(d) => {DISTRICT_YTD.byDistrict(d).where(.D_YTD > 0).map((x) => x.D_YTD).aggregate(0.00, (val, x) => val + x)}"
              }
            },            
            "indexes": {
              "byDistrictIdAndWarehouse": {
                "terms": [
                    {"field": ".D_ID"},
                    {"field": ".D_W_ID"}
                ]
              }
            },
            "constraints": [
              {
                "unique": [
                  {"field": ".D_W_ID"},
                  {"field": ".D_ID"}
                ]
              }
            ]         
        },
        "children": [
            {
                "name": "DISTRICT_NextOrderIdCounter",
                "schema": {
                    "history_days": 0,
                    "indexes": {
                      "byDistrict": {
                        "terms": [{"field": ".district"}],
                        "values": [{"field": ".next_order_id", "order": "desc"}]
                      }
                    },
                    "constraints": []
                },
                "fql": {
                    "query": 
                    """
                    DISTRICT_NextOrderIdCounter.create({
                      district: doc,
                      next_order_id: doc!.D_NEXT_O_ID
                    })
                    """
                } 
            },
            {
                "name": "DISTRICT_YTD",
                "schema": {
                    "history_days": 0,
                    "indexes": {
                      "byDistrict": {
                        "terms": [
                          {"field": ".district"}
                        ],
                        "values": [
                          {"field": ".shard", "order": "asc"},
                          {"field": ".D_YTD", "order": "asc"}
                        ]
                      },
                      "byDistrictAndShard": {
                        "terms": [
                          {"field": ".district"},
                          {"field": ".shard"}
                        ],
                        "values": [
                          {"field": ".D_YTD", "order": "asc"}
                        ]
                      }
                    },
                    "constraints": []
                },
                "fql": {
                    "query": 
                    """
                    list100.forEach(x => {
                      DISTRICT_YTD.create({
                        shard: x,
                        district: doc,
                        D_ID: doc!.D_ID,
                        D_W_ID: doc!.D_W_ID,
                        D_YTD: if (x == 1) { doc!.D_YTD } else { 0.00 }
                      })
                    })
                    """,
                    "arguments": [{
                        "name": "list100",
                        "value": list(range(1,101))
                    }]
                    
                }
            }
        ]
    },
    constants.TABLENAME_CUSTOMER: {
        "columns": [
            "C_ID", # INTEGER
            "C_D_ID", # TINYINT
            "C_W_ID", # SMALLINT
            "C_FIRST", # VARCHAR
            "C_MIDDLE", # VARCHAR
            "C_LAST", # VARCHAR
            "C_STREET_1", # VARCHAR
            "C_STREET_2", # VARCHAR
            "C_CITY", # VARCHAR
            "C_STATE", # VARCHAR
            "C_ZIP", # VARCHAR
            "C_PHONE", # VARCHAR
            "C_SINCE", # TIMESTAMP
            "C_CREDIT", # VARCHAR
            "C_CREDIT_LIM", # FLOAT
            "C_DISCOUNT", # FLOAT
            "C_BALANCE", # FLOAT
            "C_YTD_PAYMENT", # FLOAT
            "C_PAYMENT_CNT", # INTEGER
            "C_DELIVERY_CNT", # INTEGER
            "C_DATA", # VARCHAR
        ],
        "schema": {
            "history_days": 0,
            "indexes": {
              "byCustomerDistrictWarehouse": {
                "terms": [
                  {"field": ".C_ID"},
                  {"field": ".C_D_ID"},
                  {"field": ".C_W_ID"}
                ]
              },
              "byCustomerLastName_And_WarehouseDistrict": {
                "terms": [
                  {"field": ".C_LAST"},
                  {"field": ".C_D_ID"},
                  {"field": ".C_W_ID"}
                ]
              }
            },
            "constraints": [
              {
                "unique": [
                  {"field": ".C_ID"},
                  {"field": ".C_D_ID"},
                  {"field": ".C_W_ID"}
                ]
              },
              {
                "unique": [
                  {"field": ".C_W_ID"},
                  {"field": ".C_D_ID"},
                  {"field": ".C_LAST"},
                  {"field": ".C_FIRST"}
                ]
              }
            ]                   
        }
    },
    constants.TABLENAME_STOCK: {
        "columns": [
            "S_I_ID", # INTEGER
            "S_W_ID", # SMALLINT
            "S_QUANTITY", # INTEGER
            "S_DIST_01", # VARCHAR
            "S_DIST_02", # VARCHAR
            "S_DIST_03", # VARCHAR
            "S_DIST_04", # VARCHAR
            "S_DIST_05", # VARCHAR
            "S_DIST_06", # VARCHAR
            "S_DIST_07", # VARCHAR
            "S_DIST_08", # VARCHAR
            "S_DIST_09", # VARCHAR
            "S_DIST_10", # VARCHAR
            "S_YTD", # INTEGER
            "S_ORDER_CNT", # INTEGER
            "S_REMOTE_CNT", # INTEGER
            "S_DATA", # VARCHAR
        ],
        "schema": {
            "history_days": 0,
            "computed_fields": {
              "S_DISTS": {
                "body": "(s) => {[s.S_DIST_01, s.S_DIST_02, s.S_DIST_03, s.S_DIST_04, s.S_DIST_05, s.S_DIST_06, s.S_DIST_07, s.S_DIST_08, s.S_DIST_09, s.S_DIST_10]}"
              }
            },
            "indexes": {
              "byItemIdAndWarehouse": {
                "terms": [
                  {"field": ".S_I_ID"},
                  {"field": ".S_W_ID"}
                ]
              }
            },
            "constraints": [
              {
                "unique": [
                  {"field": ".S_I_ID"},
                  {"field": ".S_W_ID"}
                ]
              }
            ]
        }
    },
    constants.TABLENAME_ORDERS: {
        "columns": [
            "O_ID", # INTEGER
            "O_C_ID", # INTEGER
            "O_D_ID", # TINYINT
            "O_W_ID", # SMALLINT
            "O_ENTRY_D", # TIMESTAMP
            "O_CARRIER_ID", # INTEGER
            "O_OL_CNT", # INTEGER
            "O_ALL_LOCAL", # INTEGER
        ],
        "schema": {
            "history_days": 0,
            "indexes": {
              "byOidDistrictWarehouse": {
                "terms": [
                  {"field": ".O_ID"},
                  {"field": ".O_D_ID"},
                  {"field": ".O_W_ID"}
                ]
              },
              "byCidDistrictWarehouse": {
                "terms": [
                  {"field": ".O_C_ID"},
                  {"field": ".O_D_ID"},
                  {"field": ".O_W_ID"}
                ]
              }
            },
            "constraints": [
              {
                "unique": [
                  {"field": ".O_ID"},
                  {"field": ".O_D_ID"},
                  {"field": ".O_W_ID"}
                ]
              },
              {
                "unique": [
                  {"field": ".O_W_ID"},
                  {"field": ".O_D_ID"},
                  {"field": ".O_C_ID"},
                  {"field": ".O_ID"}
                ]
              }
            ]
        }
    },
    constants.TABLENAME_NEW_ORDER: {
        "columns": [
            "NO_O_ID", # INTEGER
            "NO_D_ID", # TINYINT
            "NO_W_ID", # SMALLINT
        ],
        "schema": {
            "history_days": 0,
            "indexes": {
              "byDistrictWarehouse": {
                "terms": [
                  {"field": ".NO_D_ID"},
                  {"field": ".NO_W_ID"}
                ]
              },
              "byDistrictWarehouseOrder": {
                "terms": [
                  {"field": ".NO_D_ID"},
                  {"field": ".NO_W_ID"},
                  {"field": ".NO_O_ID"}
                ]
              }
            },
            "constraints": [
              {
                "unique": [
                  {"field": ".NO_D_ID"},
                  {"field": ".NO_W_ID"},
                  {"field": ".NO_O_ID"}
                ]
              }
            ]
        }
    },
    constants.TABLENAME_ORDER_LINE: {
        "columns": [
            "OL_O_ID", # INTEGER
            "OL_D_ID", # TINYINT
            "OL_W_ID", # SMALLINT
            "OL_NUMBER", # INTEGER
            "OL_I_ID", # INTEGER
            "OL_SUPPLY_W_ID", # SMALLINT
            "OL_DELIVERY_D", # TIMESTAMP
            "OL_QUANTITY", # INTEGER
            "OL_AMOUNT", # FLOAT
            "OL_DIST_INFO", # VARCHAR
        ],
        "schema": {
            "history_days": 0,
            "indexes": {
              "byOrderDistrictWarehouse": {
                "terms": [
                  {"field": ".OL_O_ID"},
                  {"field": ".OL_D_ID"},
                  {"field": ".OL_W_ID"}
                ],
                "values": [
                  {"field": ".OL_AMOUNT", "order": "asc"}
                ]
              },
              "byDistrictWarehouse": {
                "terms": [
                  {"field": ".OL_D_ID"},
                  {"field": ".OL_W_ID"}
                ],
                "values": [
                  {"field": ".OL_O_ID", "order": "asc"}
                ]
              }
            },
            "constraints": [
              {
                "unique": [
                  {"field": ".OL_O_ID"},
                  {"field": ".OL_D_ID"},
                  {"field": ".OL_W_ID"},
                  {"field": ".OL_NUMBER"}
                ]
              }
            ]
        }
    },
    constants.TABLENAME_HISTORY: {
        "columns": [
            "H_C_ID", # INTEGER
            "H_C_D_ID", # TINYINT
            "H_C_W_ID", # SMALLINT
            "H_D_ID", # TINYINT
            "H_W_ID", # SMALLINT
            "H_DATE", # TIMESTAMP
            "H_AMOUNT", # FLOAT
            "H_DATA", # VARCHAR            
        ],
        "schema": {
            "history_days": 0,
            "indexes": {},            
            "constraints": []
        }
    },
}

class FaunaDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "key":             ("The api key", "secret" ),
        "fauna_url":       ("The Fauna endpoint", "https://db.fauna.com"),
        "max_batch_size":  ("Fauna API payload size limit", 250000),
        "denormalize":     ("If true, data will be denormalized using NoSQL schema design best practices", True),
    }
    DENORMALIZED_TABLES = [
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE
    ]

    def __init__(self, ddl):
        super(FaunaDriver, self).__init__("fauna", ddl)
        self.database = None
        self.client = None
        self.FAUNA_URL = "https://db.fauna.com/query/1"
        self.MAX_BATCH_SIZE_BYTES = 250000
        self.denormalize = True
        self.w_orders = {}


    def makeDefaultConfig(self):
        return FaunaDriver.DEFAULT_CONFIG


    def loadConfig(self, config):        
        self.API_KEY = config["key"]
        self.FAUNA_URL = str(config["fauna_url"]) + "/query/1"
        self.client = Client(secret=self.API_KEY,
                             client_buffer_timeout=timedelta(seconds=20),
                             http_connect_timeout=timedelta(seconds=12),
                             http_pool_timeout=timedelta(seconds=12)
                             )
        self.MAX_BATCH_SIZE_BYTES = int(config["max_batch_size"])
        self.denormalize = config['denormalize'] == 'True'

        for table in constants.ALL_TABLES:
            if "children" in TABLES[table]:
                for child in TABLES[table]["children"]:
                    schema = child["schema"]
                    schema["name"] = child["name"]
                    try :
                        res = self._loadConfigUpdateSchema(schema)
                    except Exception as err:
                        logging.error(err)
                        return
            schema = TABLES[table]["schema"]
            schema["name"] = table
            try:
                res = self._loadConfigUpdateSchema(schema)
            except Exception as err:
                logging.error(err)
                return


    def _loadConfigUpdateSchema(self, schema):
        try:
            collection_name = schema["name"]
            createColl = fql(
                """
                let c = Collection.byName(${collection_name})
                if (c == null) {
                  Collection.create(${schema})
                } else {
                  c
                }
                """,
                collection_name=collection_name,
                schema=schema)
            res: QuerySuccess = self.client.query(createColl)
            return res
        except FaunaException as err:
            logging.error("FaunaException:")
            raise Exception(err)


    def _datetimeToIsoString(self, d):
        if not isinstance(d, datetime): return d        
        isoDateStr = str(d).split(" ")
        return "%sT%sZ" % (isoDateStr[0], isoDateStr[1])


    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))

        assert tableName in TABLES, "Unexpected table %s" % tableName

        columns = TABLES[tableName]["columns"]
        num_columns = range(len(columns))

        for t in tuples:
            for idx, field in enumerate(t):
                if isinstance(field, datetime):
                    t[idx] = self._datetimeToIsoString(field)
        
        if self.denormalize and tableName in FaunaDriver.DENORMALIZED_TABLES:
            if tableName == constants.TABLENAME_ORDERS:
                for t in tuples:
                    key = tuple(t[:1]+t[2:4]) # O_ID, O_C_ID, O_D_ID, O_W_ID
                    # self.w_orders[key] = dict([(columns[i], t[i]) for i in num_columns])
                    self.w_orders[key] = dict(map(lambda i: (columns[i], t[i]), num_columns))
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    assert o_key in self.w_orders, "Order Key: %s\nAll Keys:\n%s" % (str(o_key), "\n".join(map(str, sorted(self.w_orders.keys()))))
                    o = self.w_orders[o_key]
                    if not "O_ORDER_LINES" in o:
                        o["O_ORDER_LINES"] = []
                    o["O_ORDER_LINES"].append(dict([(columns[i], t[i]) for i in num_columns[4:]]))

            ## Otherwise nothing
            else: assert False, "Only Orders and order lines are denormalized! Got %s." % tableName
        else:
            batches = []
            tuple_dicts = []
            size = 0
            for t in tuples:
                doc = dict(map(lambda i: (columns[i], t[i]), num_columns))
                size += getsizeof(doc)
                if size > self.MAX_BATCH_SIZE_BYTES:
                    size = 0
                    batches.append(tuple_dicts)
                    tuple_dicts = []
                tuple_dicts.append(doc)
            batches.append(tuple_dicts)

            args = {
                "coll_name": tableName
            }
            fql = [
                "docs.forEach(data => {\n",
                "  let doc = Collection(coll_name).create(data)\n"
            ]
            if "children" in TABLES[tableName]:
                for child in TABLES[tableName]["children"]:
                    if "fql" in child:
                        fql.append(child["fql"]["query"])
                        if "arguments" in child["fql"]:
                            for arg in child["fql"]["arguments"]:
                                args[arg["name"]] = arg["value"]
            fql.append("  null")
            fql.append("})")

            try:
                for docs in batches:
                    args["docs"] = docs
                    r = requests.post(self.FAUNA_URL, 
                                      json={
                                          "query": { "fql": fql },
                                          "arguments": args
                                      },
                                      headers={
                                          "Authorization": "Bearer {}".format(self.API_KEY),
                                          "Content-Type": "application/json"
                                      })
                    response = json.loads(r.text)
                    if "stats" in response:
                        print(response["stats"])
                    else:
                        print(response)
            except Exception as err:
                logging.error(err)
                return


    def loadFinishDistrict(self, w_id, d_id):
        if self.denormalize:
            logging.debug("Pushing %d denormalized ORDERS records for WAREHOUSE %d DISTRICT %d", len(self.w_orders), w_id, d_id)
            batches = []
            tuple_dicts = []
            size = 0
            for doc in self.w_orders.values():
                size += getsizeof(doc)
                if size > self.MAX_BATCH_SIZE_BYTES:
                    size = 0
                    batches.append(tuple_dicts)
                    tuple_dicts = []
                tuple_dicts.append(doc)
            batches.append(tuple_dicts)            
            self.w_orders.clear()

            args = {
                "coll_name": constants.TABLENAME_ORDERS
            }
            fql = [
                "docs.forEach(data => {\n",
                "  Collection(coll_name).create(data)\n",
                "})\n"
            ]
            try:
                for docs in batches:
                    args["docs"] = docs
                    r = requests.post(self.FAUNA_URL, 
                                      json={
                                          "query": { "fql": fql },
                                          "arguments": args
                                      },
                                      headers={
                                          "Authorization": "Bearer {}".format(self.API_KEY),
                                          "Content-Type": "application/json"
                                      })
                    response = json.loads(r.text)
                    if "stats" in response:
                        print(response["stats"])
                    else:
                        print(response)
            except Exception as err:
                logging.error(err)
                return


    def returnResult(self, res):
        return (res, 0)


    def doDelivery(self, params):        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = self._datetimeToIsoString(params["ol_delivery_d"])

        districts = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            districts.append(d_id)

        result = [ ]
        try:
            if self.denormalize:
                q_update_deliver_date = fql("""
                                            let ol = o!.O_ORDER_LINES
                                            let updated_order = {
                                              O_CARRIER_ID: ${o_carrier_id},
                                              O_DELIVERY_D: ${ol_delivery_d}
                                            }
                                            """,
                                            w_id=w_id,
                                            ol_delivery_d=ol_delivery_d,
                                            o_carrier_id=o_carrier_id)                                                            
            else:
                q_update_deliver_date = fql("""
                                            let ol = ORDER_LINE.byOrderDistrictWarehouse(no_o_id, d_id, ${w_id})
                                            ol.forEach(x => {
                                              x.update({
                                                OL_DELIVERY_D: ${ol_delivery_d}
                                              })
                                            })
                                            let updated_order = {
                                              O_CARRIER_ID: ${o_carrier_id}
                                            }
                                            """,
                                            w_id=w_id,
                                            ol_delivery_d=ol_delivery_d,
                                            o_carrier_id=o_carrier_id)
            q = fql("""
                    let newOrders = ${districts}.map(d_id => {
                      let no_doc = NEW_ORDER.byDistrictWarehouse(d_id, ${w_id}).where(.NO_O_ID > -1).first()
                      let no_o_id = no_doc?.NO_O_ID

                      // delete the new order
                      no_doc?.delete()
                    
                      // return new order order id and district id
                      {
                        no_o_id: no_o_id,
                        d_id: d_id
                      }
                    })
                    // filter out nulls
                    let newOrders = newOrders.filter(x => x.no_o_id != null)

                    newOrders.forEach(newOrder => {                    
                      let no_o_id = newOrder.no_o_id
                      let d_id = newOrder.d_id
                      let o = ORDERS.byOidDistrictWarehouse(no_o_id, d_id, ${w_id}).first()

                      // populate the delivery date
                      ${q_update_deliver_date}

                      // update the carrier id and return the customer's id
                      let c_id = o!.update(updated_order).O_C_ID
                                        
                      // calculate the total amount
                      let ol_total = Math.round(ol.map(x => x.OL_AMOUNT).aggregate(0, (a, b) => a+b), 2)
                      if (ol_total > 0) {
                        CUSTOMER.byCustomerDistrictWarehouse(c_id, d_id, ${w_id}).first()!.update({
                          C_BALANCE: ol_total
                        })
                      }                      
                    })

                    // return new order ids
                    newOrders
                    """,
                    districts=districts,
                    w_id=w_id,
                    o_carrier_id=o_carrier_id,
                    ol_delivery_d=ol_delivery_d,
                    q_update_deliver_date=q_update_deliver_date
                    )
            res: QuerySuccess = self.client.query(q, QueryOptions(query_tags={"operation": "doDelivery"}))
            for newOrder in res.data:
                result.append((newOrder["d_id"], newOrder["no_o_id"]))
        except FaunaException as err:          
            logging.error("FaunaException:")
            logging.error(err)
            return
            
        return self.returnResult(result)


    def doNewOrder(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = self._datetimeToIsoString(params["o_entry_d"])
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        assert i_ids, "No matching i_ids found for new order"
        assert len(i_ids) == len(i_w_ids), "different number of i_ids and i_w_ids"
        assert len(i_ids) == len(i_qtys), "different number of i_ids and i_qtys"

        all_local = list(set(i_w_ids)) == [w_id]

        try:
            if self.denormalize:
                q_order_line_create = fql("")
                q_order_lines_denorm_append = fql("""
                                                  val.order_lines.append({
                                                    OL_NUMBER: ol_number,
                                                    OL_I_ID: ol_i_id,
                                                    OL_SUPPLY_W_ID: ol_supply_w_id, 
                                                    OL_QUANTITY: ol_quantity, 
                                                    OL_AMOUNT: ol_amount, 
                                                    OL_DIST_INFO: s_dist_xx
                                                  })
                                                  """)
                q_denorm_order_lines = fql("""
                                           {
                                              O_ORDER_LINES: info.order_lines
                                           }
                                           """)
            else:
                q_order_line_create = fql("""
                                          ORDER_LINE.create({
                                            OL_O_ID: d_next_o_id, 
                                            OL_D_ID: ${d_id}, 
                                            OL_W_ID: ${w_id}, 
                                            OL_NUMBER: ol_number, 
                                            OL_I_ID: ol_i_id, 
                                            OL_SUPPLY_W_ID: ol_supply_w_id, 
                                            OL_DELIVERY_D: ${o_entry_d}, 
                                            OL_QUANTITY: ol_quantity, 
                                            OL_AMOUNT: ol_amount, 
                                            OL_DIST_INFO: s_dist_xx
                                          })
                                          """)                
                q_order_lines_denorm_append = fql("[]")
                q_denorm_order_lines = fql("""
                                           {
                                              O_ORDER_LINES: null
                                           }
                                           """)

            q = fql("""
                    let items = ${i_ids}.map(x => {
                      let i = ITEM.byItemId(x).first()
                      {
                        name: i?.I_NAME,
                        price: i?.I_PRICE,
                        data: i?.I_DATA
                      }
                    })
                    let items = items.filter(x => x.name != null)
                    let ol_cnt = ${i_ids}.length
                    if (items.length != ol_cnt) {
                      abort("wrong item id was specified")
                    }

                    let w_tax = WAREHOUSE.byWarehouseId(${w_id}).first()!.W_TAX

                    let district = DISTRICT.byDistrictIdAndWarehouse(${d_id}, ${w_id}).first()
                    let d_tax = district!.D_TAX
                    let district_counter = DISTRICT_NextOrderIdCounter.byDistrict(district).first()
                    let d_next_o_id = district_counter!.next_order_id

                    // --------------------------------
                    // Insert Order Information
                    // --------------------------------
                    // Increment the counter
                    DISTRICT_NextOrderIdCounter.create({
                      district: district,
                      next_order_id: d_next_o_id + 1
                    })
                    district_counter!.update({
                      ttl: Time.now().add(1, "minute")
                    })

                    // Create New Order
                    NEW_ORDER.create({
                      NO_O_ID: d_next_o_id,
                      NO_D_ID: ${d_id},
                      NO_W_ID: ${w_id}
                    })

                    // --------------------------------
                    // Insert Order Item Information
                    // --------------------------------
                    let info = items.entries().fold(
                      {
                        total: 0,
                        item_data: [],
                        order_lines: []
                      }, 
                      (val, x) => {
                        let i: Any = x[0]
                        let ol_number = i + 1
                        let ol_supply_w_id = ${i_w_ids}[i]
                        let ol_i_id = ${i_ids}[i]
                        let ol_quantity = ${i_qtys}[i]

                        let item: Any = x[1]
                        let i_name = item.name
                        let i_data = item.data
                        let i_price = item.price
                      
                        let stock = STOCK.byItemIdAndWarehouse(ol_i_id, ol_supply_w_id).first()
                        if (stock == null) {
                          abort("No STOCK record for (ol_i_id=" + ol_i_id.toString() + ", ol_supply_w_id=" + ol_supply_w_id.toString() + ")")
                        }
                      
                        let s_quantity = stock!.S_QUANTITY
                        let s_ytd = stock!.S_YTD
                        let s_order_cnt = stock!.S_ORDER_CNT
                        let s_remote_cnt = stock!.S_REMOTE_CNT
                        let s_data = stock!.S_DATA
                        let s_dist_xx = stock!.S_DISTS[${d_id} - 1]
                      
                        let s_ytd = s_ytd + ol_quantity
                        let s_quantity = if (s_quantity >= ol_quantity + 10) {
                          s_quantity - ol_quantity
                        } else {
                          s_quantity + 91 - ol_quantity
                        }
                        let s_order_cnt = s_order_cnt + 1
                        let s_remote_cnt = if (ol_supply_w_id != ${w_id}) {
                          s_remote_cnt + 1
                        } else {
                          s_remote_cnt
                        }
                      
                        stock!.update({
                          S_QUANTITY: s_quantity,
                          S_YTD: s_ytd,
                          S_ORDER_CNT: s_order_cnt,
                          S_REMOTE_CNT: s_remote_cnt
                        })
                      
                        let brand_generic = if (i_data.includes(${string_original}) && i_data.includes(${string_original})) {
                          "B"
                        } else {
                          "G"
                        }
                      
                        let ol_amount = Math.round(ol_quantity * i_price, 2)
                      
                        ${q_order_line_create}
                      
                        {
                          total: val.total + ol_amount,
                          item_data: val.item_data.append({
                            i_name: i_name,
                            s_quantity: s_quantity, 
                            brand_generic: brand_generic, 
                            i_price: i_price, 
                            ol_amount: ol_amount                    
                          }),
                          order_lines: ${q_order_lines_denorm_append}
                        }
                      }
                    )

                    // Create Order
                    ORDERS.createData(
                      Object.assign(
                        {
                          O_ID: d_next_o_id, 
                          O_D_ID: ${d_id}, 
                          O_W_ID: ${w_id}, 
                          O_C_ID: ${c_id}, 
                          O_ENTRY_D: ${o_entry_d}, 
                          O_CARRIER_ID: ${o_carrier_id}, 
                          O_OL_CNT: ol_cnt, 
                          O_ALL_LOCAL: ${all_local}
                        },
                        ${q_denorm_order_lines}
                      )
                    )

                    let customer = CUSTOMER.byCustomerDistrictWarehouse(${c_id}, ${d_id}, ${w_id}).first() {
                      C_DISCOUNT, C_LAST, C_CREDIT
                    }
                    
                    Object.assign({
                      customer_info: customer!,
                      w_tax: w_tax,
                      d_tax: d_tax,
                      d_next_o_id: d_next_o_id
                    }, info)
                    """,
                    i_ids=i_ids,
                    i_w_ids=i_w_ids,
                    i_qtys=i_qtys,
                    w_id=w_id,
                    d_id=d_id,
                    c_id=c_id,
                    o_entry_d=o_entry_d,
                    o_carrier_id=constants.NULL_CARRIER_ID,
                    all_local=all_local,
                    string_original=constants.ORIGINAL_STRING,
                    q_order_line_create=q_order_line_create,
                    q_order_lines_denorm_append=q_order_lines_denorm_append,
                    q_denorm_order_lines=q_denorm_order_lines
                    )
            res: QuerySuccess = self.client.query(q, QueryOptions(query_tags={"operation": "doNewOrder"}))
            data = res.data
            stats: QueryStats = res.stats
            print(stats)
            item_data = data["item_data"]
            customer_info = data["customer_info"]
            ## Pack up values the client is missing (see TPC-C 2.4.3.5)
            c_discount = data["customer_info"]["C_DISCOUNT"]
            w_tax = data["w_tax"]
            d_tax = data["d_tax"]
            total = data["total"] * (1 - c_discount) * (1 + w_tax + d_tax)
            misc = {
                "w_tax": w_tax,
                "d_tax": d_tax,
                "next_o_id": data["d_next_o_id"],
                "total": total
            }
            return self.returnResult([ customer_info, misc, item_data ])
        except AbortError as err:
            logging.debug(err.abort)
            return self.returnResult(None)
        except FaunaException as err:
            logging.error("FaunaException:")
            logging.error(err)
            return


    def doOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        try:
            if self.denormalize:
                q_order_line_denorm = fql("""
                                          let o = ORDERS.byCidDistrictWarehouse(c_id, ${d_id}, ${w_id}).first() {
                                            O_ID, O_CARRIER_ID, O_ENTRY_D, O_ORDER_LINES
                                          }
                                          let ol = o!.O_ORDER_LINES
                                          """,
                                          d_id=d_id,
                                          w_id=w_id)                
            else:
                q_order_line_denorm = fql("""
                                          let o = ORDERS.byCidDistrictWarehouse(c_id, ${d_id}, ${w_id}).first() {
                                            O_ID, O_CARRIER_ID, O_ENTRY_D
                                          }
                                          let ol = (ORDER_LINE.byOrderDistrictWarehouse(o!.O_ID, ${d_id}, ${w_id}) {
                                            OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
                                          }).toArray()
                                          """,
                                          d_id=d_id,
                                          w_id=w_id)

            q = fql("""
                    let c = if (${cid} != null) {
                      CUSTOMER.byCustomerDistrictWarehouse(${cid}, ${d_id}, ${w_id}).first() {
                        C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
                      }
                    } else {
                      let customers = CUSTOMER.byCustomerLastName_And_WarehouseDistrict(${c_last}, ${d_id}, ${w_id}) {
                        C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
                      }                    
                      customers.toArray()[Math.floor( customers.count()/2 )]
                    }
                    let c_id = c!.C_ID

                    ${q_order_line_denorm}

                    {
                      customer: c,
                      order: o,
                      orderLines: ol
                    }
                    """,
                    cid=c_id,
                    c_last=c_last,
                    d_id=d_id,
                    w_id=w_id,
                    q_order_line_denorm=q_order_line_denorm
                  )
            res: QuerySuccess = self.client.query(q, QueryOptions(query_tags={"operation": "doOrderStatus"}))
            data = res.data
            return self.returnResult([ data["customer"], data["order"], data["orderLines"] ])
        except FaunaException as err:
            logging.error("FaunaException:")
            logging.error(err)
            return


    def doPayment(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = self._datetimeToIsoString(params["h_date"])
        w_shard = random.randint(1, 1000)
        d_shard = random.randint(1, 100)

        try:
            q = fql("""
                    let c = if (${cid} != null) {
                      CUSTOMER.byCustomerDistrictWarehouse(${cid}, ${d_id}, ${w_id}).first()
                    } else {
                      let customers = CUSTOMER.byCustomerLastName_And_WarehouseDistrict(${c_last}, ${d_id}, ${w_id})
                      customers.toArray()[Math.floor( customers.count()/2 )]
                    }
                    let c_id = c!.C_ID
                    let c_balance = c!.C_BALANCE - ${h_amount}
                    let c_ytd_payment = c!.C_YTD_PAYMENT + ${h_amount}
                    let c_payment_cnt = c!.C_PAYMENT_CNT + 1
                    let c_data = c!.C_DATA

                    let w = WAREHOUSE.byWarehouseId(${w_id}).first()
                    let w_ytd = WAREHOUSE_YTD.byWarehouseAndShard(w, ${w_shard}).first()
                    w_ytd!.update({
                      W_YTD: w_ytd!.W_YTD + ${h_amount}
                    })
                    let w = w {
                      W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
                    }

                    let d = DISTRICT.byDistrictIdAndWarehouse(${d_id}, ${w_id}).first()
                    let d_ytd = DISTRICT_YTD.byDistrictAndShard(d, ${d_shard}).first()
                    d_ytd!.update({
                      D_YTD: d_ytd!.D_YTD + ${h_amount}
                    })
                    let d = d {
                      D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
                    }

                    let c = if (c!.C_CREDIT == ${bad_credit}) {
                      let newData: Any = [c_id, ${c_d_id}, ${c_w_id}, ${d_id}, ${w_id}, ${h_amount}].reduce((prev, curr) => {
                        let left: String = prev.toString()
                        let right: String = curr.toString()
                        left + " " + right
                      })
                      let c_data = newData + "|" + c_data
                      c!.update({
                        C_BALANCE: c_balance,
                        C_YTD_PAYMENT: c_ytd_payment,
                        C_PAYMENT_CNT: c_payment_cnt,
                        C_DATA: c_data.slice(0, ${max_c_data} + 1)
                      }) {
                        C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
                      }                
                    } else {
                      c!.update({
                        C_BALANCE: c_balance,
                        C_YTD_PAYMENT: c_ytd_payment,
                        C_PAYMENT_CNT: c_payment_cnt,
                      }) {
                        C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
                      }            
                    }

                    HISTORY.create({
                      H_C_ID: c_id, 
                      H_C_D_ID: ${c_d_id}, 
                      H_C_W_ID: ${c_w_id}, 
                      H_D_ID: ${d_id}, 
                      H_W_ID: ${w_id}, 
                      H_DATE: ${h_date}, 
                      H_AMOUNT: ${h_amount}, 
                      H_DATA: w!.W_NAME + "    " + d!.D_NAME
                    })

                    {
                      customer: c,
                      warehouse: w,
                      district: d
                    }
                    """,
                    w_id=w_id,
                    d_id=d_id,
                    cid=c_id,
                    c_last=c_last,
                    c_d_id=c_d_id,
                    c_w_id=c_w_id,
                    h_amount=h_amount,
                    h_date=h_date,
                    bad_credit=constants.BAD_CREDIT,
                    max_c_data=constants.MAX_C_DATA,
                    w_shard=w_shard,
                    d_shard=d_shard
                  )
            
            res: QuerySuccess = self.client.query(q, QueryOptions(query_tags={"operation": "doPayment"}))
            data = res.data
            return self.returnResult([ data["warehouse"], data["district"], data["customer"] ])
        except FaunaException as err:
            logging.error("FaunaException:")
            logging.error(err)
            return


    def doStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        try:
            if self.denormalize:
                q = fql("""
                        let district = DISTRICT.byDistrictIdAndWarehouse(${d_id}, ${w_id}).first()
                        let o_id = DISTRICT_NextOrderIdCounter.byDistrict(district).first()!.next_order_id
                        ${twenty}.map(x => {
                          let oid = o_id - (x+1)
                          ORDERS.byOidDistrictWarehouse(oid, ${d_id}, ${w_id}).first()!.O_ORDER_LINES
                        })
                        .flatten()
                        .distinct()
                        .map(x => {
                          STOCK.byItemIdAndWarehouse(x, 1).where(.S_QUANTITY < 20).count()
                        })
                        .filter(x => { x > 0 }).length
                        """,
                        w_id=w_id,
                        d_id=d_id,
                        threshold=threshold,
                        twenty=list(range(0,20))
                      )                
            else:
                q = fql("""
                        let district = DISTRICT.byDistrictIdAndWarehouse(${d_id}, ${w_id}).first()
                        let o_id = DISTRICT_NextOrderIdCounter.byDistrict(district).first()!.next_order_id

                        ORDER_LINE.byDistrictWarehouse(${d_id}, ${w_id}, { 
                          from: o_id - 20,
                          to: o_id - 1
                        }).map(ol => {
                          let s = STOCK.byItemIdAndWarehouse(ol.OL_I_ID, ${w_id}).where(.S_QUANTITY < ${threshold})
                          s.toArray()
                        }).toArray().flatten().distinct().length
                        """,
                        w_id=w_id,
                        d_id=d_id,
                        threshold=threshold
                      )
            res: QuerySuccess = self.client.query(q, QueryOptions(query_tags={"operation": "doStockLevel"}))
            return self.returnResult(int(res.data))
        except FaunaException as err:
            logging.error("FaunaException:")
            logging.error(err)
            return

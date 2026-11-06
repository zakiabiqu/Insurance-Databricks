from insurance.config import _deep_merge, load_config, out_table, src_table
 
 
def test_deep_merge_keeps_unchanged_keys():
    merged = _deep_merge({"target": {"schema": "ws", "tables": {"a": "x"}}},
                         {"target": {"schema": "ws_dev"}})
    assert merged["target"] == {"schema": "ws_dev", "tables": {"a": "x"}}
 
 
def test_each_env_has_own_catalog():
    assert load_config("dev")["catalog"] == "insurance_dev"
    assert load_config("prd")["catalog"] == "insurance_prd"
 
 
def test_table_names():
    cfg = load_config("dev")
    assert src_table(cfg, "policy") == "insurance_dev.dm_analytics.dm_policy_performance"
    assert out_table(cfg, "policy_portfolio") == "insurance_dev.ws_insurance.ins_policy_portfolio"
## Fault tolerance script instructions

### From root directory:

* Example run without failures (adjusts flags as needed)

```python
python3 scripts/test_tpv_sharded_fault_tolerance.py --build --force-recreate --skip-image-pull --failure-rate 0.0
```

* *Example run with failures (adjusts flags as needed)

```python
python3 scripts/test_tpv_sharded_fault_tolerance.py --build --force-recreate --skip-image-pull --client-timeout 1500 --failure-rate 0.8 --duration 30 --interval 60
```
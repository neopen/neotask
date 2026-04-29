#!/bin/bash
# 异常场景测试运行脚本

echo "=========================================="
echo "NeoTask 异常场景测试"
echo "=========================================="

# 运行所有异常测试
echo ""
echo="运行异常场景测试..."
pytest tests/chaos/test_exception_scenarios.py -m chaos -v --maxfail=3

# 运行特定测试
echo ""
echo "运行分布式锁异常测试..."
pytest tests/chaos/test_exception_scenarios.py::TestDistributedLockFailures -v

echo ""
echo "运行竞态条件测试..."
pytest tests/chaos/test_exception_scenarios.py::TestRaceConditions -v

echo ""
echo "异常测试完成"
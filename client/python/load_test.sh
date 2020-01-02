export CONDUCTOR_API="$(k get ing conductor-server -o jsonpath='{.spec.rules[].host}')/api"
python kitchensink_workers.py > worker.log &
python load_test_kitchen_sink.py
cat worker.log
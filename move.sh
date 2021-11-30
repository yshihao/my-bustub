# cp ./src/buffer/buffer_pool_manager.cpp /Users/yushihao/Documents/others/test/src/buffer/
# cp ./src/buffer/lru_replacer.cpp /Users/yushihao/Documents/others/test/src/buffer/
# cp ./src/include/buffer/buffer_pool_manager.h /Users/yushihao/Documents/others/test/src/include/buffer/
# cp ./src/include/buffer/lru_replacer.h /Users/yushihao/Documents/others/test/src/include/buffer/

# cp ./src/storage/page/b_plus_tree_internal_page.cpp ./src/storage/page/b_plus_tree_leaf_page.cpp ./src/storage/page/b_plus_tree_page.cpp /Users/yushihao/Documents/others/test/src/storage/page/
# cp ./src/storage/index/b_plus_tree.cpp ./src/storage/index/index_iterator.cpp /Users/yushihao/Documents/others/test/src/storage/index/
# cp ./src/include/storage/page/b_plus_tree_internal_page.h ./src/include/storage/page/b_plus_tree_leaf_page.h ./src/include/storage/page/b_plus_tree_page.h /Users/yushihao/Documents/others/test/src/include/storage/page/
# cp ./src/include/storage/index/b_plus_tree.h ./src/include/storage/index/index_iterator.h /Users/yushihao/Documents/others/test/src/include/storage/index/

source_dir=(
src/include/catalog/catalog.h 
src/include/execution/execution_engine.h 
src/include/execution/executor_factory.h 
src/include/execution/executors/seq_scan_executor.h
src/include/execution/executors/index_scan_executor.h
src/include/execution/executors/insert_executor.h
src/include/execution/executors/update_executor.h
src/include/execution/executors/delete_executor.h
src/include/execution/executors/nested_loop_join_executor.h
src/include/execution/executors/nested_index_join_executor.h
src/include/execution/executors/limit_executor.h
src/include/execution/executors/aggregation_executor.h
src/include/storage/index/b_plus_tree_index.h
src/include/storage/index/index.h
src/execution/executor_factory.cpp
src/execution/seq_scan_executor.cpp
src/execution/index_scan_executor.cpp
src/execution/insert_executor.cpp
src/execution/update_executor.cpp
src/execution/delete_executor.cpp
src/execution/nested_loop_join_executor.cpp
src/execution/nested_index_join_executor.cpp
src/execution/limit_executor.cpp
src/execution/aggregation_executor.cpp
src/storage/index/b_plus_tree_index.cpp
)



# cd /Users/yushihao/Documents/others/test
# rm solution3.ziprm
# zip -r solution3.zip src 
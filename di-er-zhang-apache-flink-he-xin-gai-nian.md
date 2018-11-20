1. 纯流式计算
2. Exactly once 语义（一致性CP和State管理）
3. 统一的SQL&TableAPI，以及统一的底层引擎
4. WaterMark数据对齐，解决LateEvent问题
5. 高性能，介绍查询优化器和本地状态管理 Stateful Flink applications are optimized for local state access. Task state is always maintained in memory or, if the state size exceeds the available memory, in access-efficient on-disk data structures.




use aerugo_cache::storage::MemoryStore;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

fn storage_set_get(criterion: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    criterion.bench_function("storage_set_get", |bencher| {
        bencher.iter(|| {
            runtime.block_on(async {
                let store = MemoryStore::new();
                store
                    .set("project".to_string(), b"aerugo-cache".to_vec())
                    .await
                    .unwrap();

                black_box(store.get("project").await.unwrap());
            });
        });
    });
}

fn storage_list_push_range(criterion: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    criterion.bench_function("storage_list_push_range", |bencher| {
        bencher.iter(|| {
            runtime.block_on(async {
                let store = MemoryStore::new();
                store
                    .rpush(
                        "events".to_string(),
                        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
                    )
                    .await
                    .unwrap();

                black_box(store.lrange("events", 0, -1).await.unwrap());
            });
        });
    });
}

criterion_group!(benches, storage_set_get, storage_list_push_range);
criterion_main!(benches);

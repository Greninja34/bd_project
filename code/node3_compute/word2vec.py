"""Node 3 - NLP twist: learn word vectors from the corpus with Spark MLlib Word2Vec, and store each
word's nearest neighbours for query expansion on Node 4.

    spark-submit --properties-file ~/spark-config/search.conf \
        --conf spark.pyspark.driver.python=$HOME/nlp-venv/bin/python \
        code/node3_compute/word2vec.py --phase 0

(pyspark.ml needs numpy on the DRIVER only - hence the nlp-venv. The executors keep plain Python
3.10: they only run the tokenizer UDF; the Word2Vec training itself runs in the JVM.)

How it works
  1. tokenize every doc on the executors (the same textproc.doc_terms as the index)
  2. Word2Vec (skip-gram): each word gets a 100-dim vector so that words used in similar contexts
     get similar vectors; trained in parallel over `--partitions` partitions on the workers
  3. on the driver: cosine similarity of every word with every other word (numpy, in chunks),
     keep the top `--neighbours` per word
  4. write search.synonyms (term, neighbour, similarity, rank, docs_snapshot_id)

It is optional: the export includes the synonyms when they exist, and Node 4's `expand=1` uses them.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import lake  # noqa: E402
from lake import t  # noqa: E402

PROBES = ["disease", "war", "ocean", "music", "king", "computer", "river", "planet", "flower", "football"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, default=-1)
    ap.add_argument("--vector-size", type=int, default=100)
    ap.add_argument("--min-count", type=int, default=5, help="ignore words seen fewer times")
    ap.add_argument("--window", type=int, default=5)
    ap.add_argument("--iterations", type=int, default=5)
    ap.add_argument("--partitions", type=int, default=2)
    ap.add_argument("--neighbours", type=int, default=5)
    args = ap.parse_args()

    import numpy as np
    from pyspark.ml.feature import Word2Vec
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    spark = lake.get_spark("word2vec")
    t0 = time.time()
    snap = lake.current_snapshot(spark, "docs")

    def _terms(title, text):
        import textproc
        return textproc.doc_terms(title, text)

    terms_udf = F.udf(_terms, ArrayType(StringType()))
    corpus = (spark.table(t("docs")).select(terms_udf("title", "text").alias("terms"))
              .repartition(args.partitions).cache())
    n_tokens = corpus.select(F.sum(F.size("terms"))).collect()[0][0]

    w2v = Word2Vec(vectorSize=args.vector_size, minCount=args.min_count, windowSize=args.window,
                   maxIter=args.iterations, numPartitions=args.partitions, seed=42,
                   inputCol="terms", outputCol="vec")
    t1 = time.time()
    model = w2v.fit(corpus)
    train_s = time.time() - t1

    rows = model.getVectors().collect()                      # (word, vector): ~10k x 100 floats
    words = [r["word"] for r in rows]
    M = np.array([r["vector"].toArray() for r in rows], dtype=np.float32)
    M /= np.linalg.norm(M, axis=1, keepdims=True) + 1e-9      # unit vectors -> dot product = cosine
    k = args.neighbours
    out = []
    for start in range(0, len(words), 1000):                 # chunks keep memory at ~40 MB
        sims = M[start:start + 1000] @ M.T
        for i in range(sims.shape[0]):
            sims[i, start + i] = -1.0                        # not your own neighbour
        top = np.argpartition(-sims, k, axis=1)[:, :k]
        for i in range(sims.shape[0]):
            order = top[i][np.argsort(-sims[i, top[i]])]
            for rank, j in enumerate(order, 1):
                out.append((words[start + i], words[j], float(sims[i, j]), rank))

    from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
    schema = StructType([StructField("term", StringType()), StructField("neighbour", StringType()),
                         StructField("similarity", DoubleType()), StructField("rank", IntegerType())])
    syn = (spark.createDataFrame(out, schema)
           .withColumn("docs_snapshot_id", F.lit(snap).cast("long")))
    if lake.table_exists(spark, "synonyms"):
        syn.writeTo(t("synonyms")).overwrite(F.lit(True))
    else:
        syn.writeTo(t("synonyms")).using("iceberg").create()

    lookup = {}
    for term, nb, sim, rank in out:
        if rank <= 4:
            lookup.setdefault(term, []).append(f"{nb} {sim:.2f}")
    lake.banner("WORD2VEC", [
        ("docs snapshot", snap), ("training tokens", n_tokens),
        ("vocabulary (min count %d)" % args.min_count, len(words)),
        ("vector size / window / iterations / partitions",
         f"{args.vector_size} / {args.window} / {args.iterations} / {args.partitions}"),
        ("neighbour rows written", len(out)), ("training seconds", round(train_s, 1)),
        ("total seconds", round(time.time() - t0, 1))] +
        [(f"  {w}", ", ".join(lookup[w])) for w in PROBES if w in lookup])
    lake.log_run(spark, "word2vec", args.phase, snap, {
        "vocab": len(words), "tokens": n_tokens, "vector_size": args.vector_size,
        "iterations": args.iterations, "partitions": args.partitions})
    spark.stop()


if __name__ == "__main__":
    main()

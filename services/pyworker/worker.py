import json
import os
import sys
import time
from collections import Counter

from kafka import KafkaConsumer, KafkaProducer

import nltk
from nltk import word_tokenize, sent_tokenize
from nltk.sentiment import SentimentIntensityAnalyzer

import spacy


def ensure_nltk_data() -> None:
    spacy.cli.download("en_core_web_sm")
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)
    try:
        nltk.data.find("tokenizers/punkt_tab")
    except LookupError:
        nltk.download("punkt_tab", quiet=True)
    try:
        nltk.data.find("sentiment/vader_lexicon")
    except LookupError:
        nltk.download("vader_lexicon", quiet=True)


def get_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def get_env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def analyze_text_chunk(
    text: str,
    paragraph_index: int,
    top_n: int,
    sia: SentimentIntensityAnalyzer,
    original_timestamp: int,
    nlp,
) -> dict:
    # Tokenization & normalization
    tokens = word_tokenize(text)
    normalized = [t.lower() for t in tokens if any(ch.isalnum() for ch in t)]
    freq = Counter(normalized)

    word_count = sum(freq.values())

    # Sentiment
    scores = sia.polarity_scores(text)
    pos_count = int(scores["pos"] * word_count)
    neg_count = int(scores["neg"] * word_count)
    sentiment_score = int(scores["compound"] * 1000)

    # Замена имен через NER
    doc = nlp(text)
    entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]

    person_spans = [ent for ent in doc.ents if ent.label_ == "PERSON"]
    person_token_idxs = set()
    for ent in person_spans:
        for tok in ent:
            person_token_idxs.add(tok.i)

    replaced_tokens = []
    for i, tok in enumerate(doc):
        if i in person_token_idxs:
            replaced_tokens.append("NAME" + tok.whitespace_)
        else:
            replaced_tokens.append(tok.text_with_ws)
    replaced_text = "".join(replaced_tokens)

    sentences = sent_tokenize(text)
    sorted_sentences = sorted(sentences, key=len)
    top = sorted(freq.items(), key=lambda x: (-x[1], x[0]))[:top_n]
    top_n_list = [{"word": w, "count": c} for w, c in top]

    result = {
        "paragraph_index": paragraph_index,
        "word_count": word_count,
        "word_freq": dict(freq),
        "pos_count": pos_count,
        "neg_count": neg_count,
        "sentiment_score": sentiment_score,
        "replaced_text": replaced_text,
        "sorted_sentences": sorted_sentences,
        "entities": entities,
        "top_n": top_n_list,
        "top_n_size": top_n,
        "original_metadata": {},
        "original_timestamp": original_timestamp,
        "timestamp": int(time.time() * 1000),
    }
    return result


def main() -> None:
    ensure_nltk_data()

    nlp = spacy.load("en_core_web_sm")

    brokers = get_env("KAFKA_BROKERS", "localhost:19092")
    source_topic = get_env("SOURCE_TOPIC", "text-chunks")
    result_topic = get_env("RESULT_TOPIC", "text-chunks-results")
    group_id = get_env("CONSUMER_GROUP", "text-worker-group")
    top_n = get_env_int("TOP_N", 10)

    print(
        f"Python worker starting. brokers={brokers}, source={source_topic}, "
        f"result={result_topic}, group={group_id}, topN={top_n}",
        flush=True,
    )

    consumer = KafkaConsumer(
        source_topic,
        bootstrap_servers=brokers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    producer = KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sia = SentimentIntensityAnalyzer()

    paragraph_index = 0

    try:
        for msg in consumer:
            text = msg.value
            idx = paragraph_index
            paragraph_index += 1

            result = analyze_text_chunk(text, idx, top_n, sia, msg.timestamp, nlp)

            producer.send(result_topic, result)
            print(
                f"Worker={os.getenv('HOSTNAME')} partition={msg.partition} offset={msg.offset} "
                f"local_idx={idx} length={len(text)} bytes, sent to {result_topic}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("Python worker stopping due to KeyboardInterrupt", file=sys.stderr)
    finally:
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()



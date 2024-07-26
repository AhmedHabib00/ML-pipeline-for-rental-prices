#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting results to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    try:
        artifact_path = run.use_artifact(args.input_artifact).file()
        df = pd.read_csv(artifact_path)
    except wandb.CommError as e:
        logger.error(f"Error downloading artifact: {e}")
        run.finish(exit_code=1)
        return
    logger.info("Artifact downloaded successfully.")

    logger.info("Applying basic data cleaning")
    try:
        # Remove outliers
        idx = df['price'].between(args.min_price, args.max_price)
        df = df[idx].copy()
        df['last_review'] = pd.to_datetime(df['last_review'])
    except KeyError as e:
        logger.error(f"Error cleaning data: {e}")
        run.finish(exit_code=1)
        return
    logger.info("Data cleaned successfully.")
    
    file_name = "df_v2.csv"
    df.to_csv(file_name, index=False)
    logger.info("Creating an artifact")
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    try:
        artifact.add_file(file_name)
        run.log_artifact(artifact)
    except wandb.CommError as e:
        logger.error(f"Error creating artifact: {e}")
        run.finish(exit_code=1)
        return

    logger.info("Artifact created successfully.")
    logger.info("Data cleaning finished successfully.")



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help= "Raw file to get from W&B",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='Name of the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type of the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Description of the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='Minimum price to consider',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='Maximum price to consider',
        required=True
    )


    args = parser.parse_args()

    go(args)

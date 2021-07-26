def main():
    # get args
    # replace sql templates with args
    # submit sql files to athena and wait
    # generate reports (text and json)
    # upload reports to s3(docker env vars for location)
    pass


# def main():

#     args = command_line_args()
#     client = s3_client(args.localstack)
#     s3 = S3(client)
#     print(f"Bucket: '{args.bucket}', prefix: '{args.prefix}', partition: {args.partition}, "
#           f"threads: {args.threads}, multiprocessor: {args.multiprocessor}, manifests: {args.manifests}, "
#           f"date-to-add: {args.date_to_add}.")

#     full_prefix = s3.get_full_s3_prefix(args.prefix, args.date_to_add, datetime.date.today())
#     print(f"Full prefix: {full_prefix}.")

#     results = [coalesce_tranche(args, summaries) for summaries in
#                s3.object_summaries(args.bucket, full_prefix, args.summaries)]
#     end = timer()
#     print(f"Total time taken: {end - start:.2f} seconds.")
#     exit(0 if all(results) else 1)
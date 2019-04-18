#!/usr/bin/python
# -*- coding: utf-8 -*-

from pathlib import Path
import mimetypes
from botocore.exceptions import ClientError


"""Classes for S3 Buckets."""

class BucketManager:
	"""Manage S3 Bucket."""

	def __init__(self, session):
		"""Create a BucketManager object."""
		self.session = session
		self.s3 = self.session.resource('s3')
		

	def all_buckets(self):
		"""Create an iterator for all the buckets."""
		return self.s3.buckets.all()


	def all_objects(self, bucket_name):
		"""Get an iterator for all the objects in the specified bucket."""
		return self.s3.Bucket(bucket_name).objects.all()


	def init_bucket(self, bucket_name):
		"""Create a new bucket or return the existing bucket by name."""
		s3_bucket = None

		try:

			if self.session.region_name == 'us-east-1':
				s3_bucket = self.s3.create_bucket(Bucket=bucket_name)
			else:
				s3_bucket = self.s3.create_bucket(
					Bucket=bucket_name,
					CreateBucketConfiguration={'LocationConstraint': self.session.region_name}
					)

		except ClientError as error:
			if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
				s3_bucket = self.s3.Bucket(bucket_name)
			else:
				raise error

		return s3_bucket

	def set_policy(self, bucket_name):
		"""Set bucket policy to public."""
		policy = """
		{
		  "Version":"2012-10-17",
		  "Statement":[{
		  "Sid":"PublicReadGetObject",
		  "Effect":"Allow",
		  "Principal": "*",
		      "Action":["s3:GetObject"],
		      "Resource":["arn:aws:s3:::%s/*"
		      ]
		    }
		  ]
		}
		""" % bucket_name.name
		policy = policy.strip()

		pol = bucket_name.Policy()
		pol.put(Policy=policy)


	def configure_website(self, bucket_name):
		"""Configure S3 bucket for hosting."""
		ws = bucket_name.Website()
		ws.put(WebsiteConfiguration={
			'ErrorDocument': {
			'Key': 'error.html'
			},
			'IndexDocument': {
			'Suffix': 'index.html'
			}
			})



	@staticmethod
	def upload_file(bucket_name, path, key):
		"""Upload path to S3 bucket at key"""
		content_type = mimetypes.guess_type(key)[0] or 'text/html'
		return bucket_name.upload_file(path, key, ExtraArgs={'ContentType': content_type})


	def sync(self, pathname, bucket_name):
		"""Sync contents of local PATHNAME to specified S3 Bucket"""
		s3_bucket = self.s3.Bucket(bucket_name)
		root = Path(pathname).expanduser().resolve()


		def handle_repository(target):    
			for p in target.iterdir():
				if p.is_dir(): handle_repository(p)
				if p.is_file(): self.upload_file(s3_bucket, str(p), str(p.relative_to(root)))

		handle_repository(root)
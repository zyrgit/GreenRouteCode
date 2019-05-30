
"""
Defines exceptions that are thrown.
"""

class ErrorNotInCache(Exception):
	"""Represents an Exception because mm.get is None"""
	def __init__(self, message=None):
		self.message = "" if message is None else message
	def __str__(self):
		return "[ ErrorNotInCache ] %s" % (self.message)


class ErrorTaskGiveUp(Exception):
	"""Represents an Exception when best attempt fails."""
	def __init__(self, message=None):
		self.message = "" if message is None else message
	def __str__(self):
		return "[ ErrorTaskGiveUp ] %s" % (self.message)


from email_validator import validate_email, EmailNotValidError

email = "my+address@exampleorg"

try:
  if validate_email(email, check_deliverability=False):
    print("yay its a pass")

except EmailNotValidError as e:
  # not a valid (or deliverable) email address.
  print(str(e))
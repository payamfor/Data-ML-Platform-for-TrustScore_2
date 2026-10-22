# Separate alert topics per layer.
#
# SNS bills per publish and per delivery, not per topic, so three topics cost
# the same as one carrying the same traffic. The reason to split them is
# subscription routing: Bronze schema drift and a Gold publish failure have
# different audiences, and separate topics express that without every
# subscriber needing a message-attribute filter.

locals {
  pipeline_alert_topics = {
    bronze = "${var.project}-${var.environment}-bronze-validation-alerts"
    silver = "${var.project}-${var.environment}-silver-pipeline-alerts"
    gold   = "${var.project}-${var.environment}-gold-pipeline-alerts"
  }

  pipeline_alert_subscriptions = {
    for pair in setproduct(
      toset(keys(local.pipeline_alert_topics)),
      toset(var.pipeline_alert_emails),
      ) : "${pair[0]}:${pair[1]}" => {
      layer = pair[0]
      email = pair[1]
    }
  }
}

resource "aws_sns_topic" "pipeline_alerts" {
  for_each = local.pipeline_alert_topics

  name = each.value
}

# SNS sends a confirmation message for each topic. Email delivery begins only
# after the recipient accepts that confirmation.
resource "aws_sns_topic_subscription" "pipeline_alert_email" {
  for_each = local.pipeline_alert_subscriptions

  topic_arn = aws_sns_topic.pipeline_alerts[each.value.layer].arn
  protocol  = "email"
  endpoint  = each.value.email
}

output "pipeline_alert_topic_arns" {
  description = "Per-layer SNS alert topic ARNs."
  value       = { for key, topic in aws_sns_topic.pipeline_alerts : key => topic.arn }
}

variable "project" {
  description = "Project name. Prefixes every resource in this root, which is what keeps two projects sharing an account from colliding."
  type        = string
  default     = "lotus"
}

variable "environment" {
  description = "Deployment stage. Drives resource naming and every lookup. This is the single value that changes on promotion to another stage."
  type        = string
  default     = "qa"
}

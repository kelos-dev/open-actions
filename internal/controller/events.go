package controller

import (
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
)

const failureEventAction = "ReportFailure"

func recordConditionWarning(recorder events.EventRecorder, object runtime.Object, before, after []metav1.Condition, conditionType string) {
	if recorder == nil {
		return
	}
	previous := meta.FindStatusCondition(before, conditionType)
	condition := meta.FindStatusCondition(after, conditionType)
	if condition == nil || apiEquality.Semantic.DeepEqual(previous, condition) {
		return
	}
	recorder.Eventf(object, nil, corev1.EventTypeWarning, condition.Reason, failureEventAction, "%s", condition.Message)
}
